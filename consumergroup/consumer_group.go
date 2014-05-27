package consumergroup

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	NoCheckout = errors.New("sarama: not checkout")
)

const (
	REBALANCE_START uint8 = iota + 1
	REBALANCE_OK
	REBALANCE_ERROR
)

type ConsumerGroupConfig struct {

	// The preempt interval when listening to a single partition of a topic.
	// After this interval, a different partition will be checked out to consume next.
	CheckoutInterval time.Duration

	// How often the current offsets should be committed to Zookeeper
	CommitInterval time.Duration

	KafkaClientConfig   *sarama.ClientConfig   // This will be passed to Sarama when creating a new Client
	KafkaConsumerConfig *sarama.ConsumerConfig // This will be passed to Sarama when creating a new Consumer

	ZookeeperTimeout time.Duration // The Zookeeper timeout.

}

// Creates a new ConsumerGroupConfig instance with sane defaults.
func NewConsumerGroupConfig() *ConsumerGroupConfig {
	return &ConsumerGroupConfig{
		ZookeeperTimeout:    1 * time.Second,
		CheckoutInterval:    1 * time.Second,
		CommitInterval:      5 * time.Second,
		KafkaClientConfig:   sarama.NewClientConfig(),
		KafkaConsumerConfig: sarama.NewConsumerConfig(),
	}
}

// Validates the ConsumerGroupConfig instance. It will return an error if a value
// is set to a value that is erroneous or does not make sense
func (cgc *ConsumerGroupConfig) Validate() error {
	if cgc.ZookeeperTimeout <= 0 {
		return errors.New("ZookeeperTimeout should have a duration > 0")
	}

	if cgc.KafkaClientConfig == nil {
		return errors.New("KafkaClientConfig is not set!")
	} else if err := cgc.KafkaClientConfig.Validate(); err != nil {
		return err
	}

	if cgc.KafkaConsumerConfig == nil {
		return errors.New("KafkaConsumerConfig is not set!")
	} else if err := cgc.KafkaConsumerConfig.Validate(); err != nil {
		return err
	}

	return nil
}

// A ConsumerGroup operates on all partitions of a single topic. The goal is to ensure
// each topic message is consumed only once, no matter of the number of consumer instances within
// a cluster, as described in: http://kafka.apache.org/documentation.html#distributionimpl.
//
// The ConsumerGroup internally creates multiple Consumer instances. It uses Zookkeper
// and follows a simple consumer rebalancing algorithm which allows all the consumers
// in a group to come into consensus on which consumer is consuming which partitions. Each
// ConsumerGroup can 'claim' 0-n partitions and will consume their messages until another
// ConsumerGroup instance with the same name joins or leaves the cluster.
//
// Unlike stated in the Kafka documentation, consumer rebalancing is *only* triggered on each
// addition or removal of consumers within the same group, while the addition of broker nodes
// and/or partition *does currently not trigger* a rebalancing cycle.
type ConsumerGroup struct {
	id, name, topic string

	config *ConsumerGroupConfig

	client *sarama.Client
	zoo    *ZK
	claims []PartitionConsumer
	wg     sync.WaitGroup

	zkchange <-chan zk.Event
	claimed  chan *PartitionConsumer
	listener chan *Notification

	events chan *sarama.ConsumerEvent

	checkout, force, stopper chan bool
}

type EventProcessor func(*sarama.ConsumerEvent) error

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topic string, zookeeper []string, config *ConsumerGroupConfig) (cg *ConsumerGroup, err error) {

	if config == nil {
		config = NewConsumerGroupConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if len(zookeeper) == 0 {
		return nil, errors.New("You need to provide at least one zookeeper node address!")
	}

	var zk *ZK
	if zk, err = NewZK(zookeeper, config.ZookeeperTimeout); err != nil {
		return nil, err
	}

	var kafkaBrokers []string
	if kafkaBrokers, err = zk.Brokers(); err != nil {
		return
	}

	var client *sarama.Client
	if client, err = sarama.NewClient(name, kafkaBrokers, config.KafkaClientConfig); err != nil {
		return
	}

	return NewConsumerGroup(client, zk, name, topic, nil, config)
}

// NewConsumerGroup creates a new consumer group for a given topic.
//
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
func NewConsumerGroup(client *sarama.Client, zoo *ZK, name string, topic string, listener chan *Notification, config *ConsumerGroupConfig) (group *ConsumerGroup, err error) {

	// Validate configuration
	if err = config.Validate(); err != nil {
		return
	} else if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	} else if name == "" {
		return nil, sarama.ConfigurationError("Empty name")
	}

	// Register consumer group
	if err = zoo.RegisterGroup(name); err != nil {
		return
	}

	var consumerID string
	consumerID, err = generateConsumerID()
	if err != nil {
		return
	}

	group = &ConsumerGroup{
		id:    consumerID,
		name:  name,
		topic: topic,

		config:   config,
		client:   client,
		zoo:      zoo,
		claims:   make([]PartitionConsumer, 0),
		listener: listener,

		stopper:  make(chan bool),
		checkout: make(chan bool),
		force:    make(chan bool),
		claimed:  make(chan *PartitionConsumer),

		events: make(chan *sarama.ConsumerEvent),
	}

	// Register itself with zookeeper
	if err = zoo.RegisterConsumer(group.name, group.id, group.topic); err != nil {
		return nil, err
	}

	go group.signalLoop()
	go group.eventLoop()
	group.wg.Add(2)
	return group, nil
}

// Checkout applies a callback function to a single partition consumer.
// Returns an error if any, but may also return a NoCheckout error to indicate
// that no partition was available. You should add an artificial delay keep your CPU cool.
func (cg *ConsumerGroup) Checkout(callback func(*PartitionConsumer) error) error {
	cg.checkout <- true
	claimed := <-cg.claimed

	if claimed == nil {
		return NoCheckout
	}

	return callback(claimed)
}

func (cg *ConsumerGroup) Stream() <-chan *sarama.ConsumerEvent {
	return cg.events
}

func (cg *ConsumerGroup) commitOffsets(offsets map[int32]int64) {
	for partition, offset := range offsets {
		if offset > 0 {
			sarama.Logger.Printf("Committing offset %d for partition %d", offset, partition)
			cg.Commit(partition, offset)
			offsets[partition] = 0
		}
	}
}

func (cg *ConsumerGroup) Process(processor EventProcessor) error {

	offsets := make(map[int32]int64)
	commitTimeout := time.After(cg.config.CommitInterval)

	for {
		select {
		case event, ok := <-cg.events:
			if ok == false {
				cg.commitOffsets(offsets)
				return nil
			}

			if err := processor(event); err != nil {
				return err
			}

			offsets[event.Partition] = event.Offset

		case <-commitTimeout:
			cg.commitOffsets(offsets)
			commitTimeout = time.After(cg.config.CommitInterval)
		}
	}
}

func (cg *ConsumerGroup) eventLoop() {
EventLoop:
	for {
		select {
		case <-cg.stopper:
			break EventLoop

		default:
			cg.Checkout(func(pc *PartitionConsumer) error {
				sarama.Logger.Printf("Checkout partition %d...", pc.partition)
				return pc.Fetch(cg.events, cg.config.CheckoutInterval)
			})
		}

	}
	cg.wg.Done()
}

// Commit manually commits an offset for a partition
func (cg *ConsumerGroup) Commit(partition int32, offset int64) error {
	return cg.zoo.Commit(cg.name, cg.topic, partition, offset)
}

// Offset manually retrives an offset for a partition
func (cg *ConsumerGroup) Offset(partition int32) (int64, error) {
	return cg.zoo.Offset(cg.name, cg.topic, partition)
}

// Claims returns the claimed partitions
func (cg *ConsumerGroup) Claims() []int32 {
	res := make([]int32, 0, len(cg.claims))
	for _, claim := range cg.claims {
		res = append(res, claim.partition)
	}
	return res
}

// Close closes the consumer group
func (cg *ConsumerGroup) Close() error {
	close(cg.stopper)
	cg.wg.Wait()
	return nil
}

// Background signal loop
func (cg *ConsumerGroup) signalLoop() {
	for {
		// If we have no zk handle, rebalance
		if cg.zkchange == nil {
			if err := cg.rebalance(); err != nil && cg.listener != nil {
				cg.listener <- &Notification{Type: REBALANCE_ERROR, Src: cg, Err: err}
			}
		}

		// If rebalance failed, check if we had a stop signal, then try again
		if cg.zkchange == nil {
			select {
			case <-cg.stopper:
				cg.stop()
				return
			case <-time.After(time.Millisecond):
				// Continue
			}
			continue
		}

		// If rebalance worked, wait for a stop signal or a zookeeper change or a fetch-request
		select {
		case <-cg.stopper:
			cg.stop()
			return
		case <-cg.force:
			cg.zkchange = nil
		case <-cg.zkchange:
			cg.zkchange = nil
		case <-cg.checkout:
			cg.claimed <- cg.nextConsumer()
		}
	}
}

/**********************************************************************
 * PRIVATE
 **********************************************************************/

// Stops the consumer group
func (cg *ConsumerGroup) stop() {
	cg.releaseClaims()
	cg.wg.Done()
}

// Checkout a claimed partition consumer
func (cg *ConsumerGroup) nextConsumer() *PartitionConsumer {
	if len(cg.claims) < 1 {
		return nil
	}

	shift := cg.claims[0]
	cg.claims = append(cg.claims[1:], shift)
	return &shift
}

// Start a rebalance cycle
func (cg *ConsumerGroup) rebalance() (err error) {
	var cids []string
	var pids []int32

	if cg.listener != nil {
		cg.listener <- &Notification{Type: REBALANCE_START, Src: cg}
	}

	// Fetch a list of consumers and listen for changes
	if cids, cg.zkchange, err = cg.zoo.Consumers(cg.name); err != nil {
		cg.zkchange = nil
		return
	}

	// Fetch a list of partition IDs
	if pids, err = cg.client.Partitions(cg.topic); err != nil {
		cg.zkchange = nil
		return
	}

	// Get leaders for each partition ID
	parts := make(partitionSlice, len(pids))
	for i, pid := range pids {
		var broker *sarama.Broker
		if broker, err = cg.client.Leader(cg.topic, pid); err != nil {
			cg.zkchange = nil
			return
		}
		defer broker.Close()
		parts[i] = partitionLeader{id: pid, leader: broker.Addr()}
	}

	if err = cg.makeClaims(cids, parts); err != nil {
		cg.zkchange = nil
		cg.releaseClaims()
		return
	}
	return
}

func (cg *ConsumerGroup) makeClaims(cids []string, parts partitionSlice) error {
	cg.releaseClaims()

	for _, part := range cg.claimRange(cids, parts) {
		pc, err := NewPartitionConsumer(cg, part.id)
		if err != nil {
			return err
		}

		err = cg.zoo.Claim(cg.name, cg.topic, pc.partition, cg.id)
		if err != nil {
			return err
		}

		cg.claims = append(cg.claims, *pc)
	}

	if cg.listener != nil {
		cg.listener <- &Notification{Type: REBALANCE_OK, Src: cg}
	}
	return nil
}

// Determine the partititons dumber to claim
func (cg *ConsumerGroup) claimRange(cids []string, parts partitionSlice) partitionSlice {
	sort.Strings(cids)
	sort.Sort(parts)

	cpos := sort.SearchStrings(cids, cg.id)
	clen := len(cids)
	plen := len(parts)
	if cpos >= clen || cpos >= plen {
		return make(partitionSlice, 0)
	}

	step := int(math.Ceil(float64(plen) / float64(clen)))
	if step < 1 {
		step = 1
	}

	last := (cpos + 1) * step
	if last > plen {
		last = plen
	}
	return parts[cpos*step : last]
}

// Releases all claims
func (cg *ConsumerGroup) releaseClaims() {
	for _, pc := range cg.claims {
		sarama.Logger.Printf("Releasing claim for partition %d...\n", pc.partition)
		pc.Close()
		cg.zoo.Release(cg.name, cg.topic, pc.partition, cg.id)
	}
	cg.claims = cg.claims[:0]
}
