package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	AlreadyClosing         = errors.New("The consumer group is already shutting down.")
	FailedToClaimPartition = errors.New("Failed to claim partition for this consumer instance. Do you have a rogue consumer running?")
	AlreadyCommitted       = errors.New("The highest seen offset was already committed")
)

type ConsumerGroupConfig struct {
	// The Zookeeper read timeout. Defaults to 1 second
	ZookeeperTimeout time.Duration

	// Zookeeper chroot to use. Should not include a trailing slash.
	// Leave this empty when your Kafka install does not use a chroot.
	ZookeeperChroot string

	KafkaClientConfig   *sarama.ClientConfig   // This will be passed to Sarama when creating a new sarama.Client
	KafkaConsumerConfig *sarama.ConsumerConfig // This will be passed to Sarama when creating a new sarama.Consumer

	ChannelBufferSize   int                 // The buffer size of the channel for the messages coming from Kafka. Zero means no buffering. Default is 256.
	CommitInterval      time.Duration       // The interval between which the prossed offsets are commited to Zookeeper.
	InitialOffsetMethod sarama.OffsetMethod // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetMethodOldest (default) or sarama.OffsetMethodNewest.
}

func NewConsumerGroupConfig() *ConsumerGroupConfig {
	return &ConsumerGroupConfig{
		ZookeeperTimeout:    1 * time.Second,
		ChannelBufferSize:   256,
		CommitInterval:      5 * time.Second,
		InitialOffsetMethod: sarama.OffsetMethodOldest,
	}
}

func (cgc *ConsumerGroupConfig) Validate() error {
	if cgc.ZookeeperTimeout <= 0 {
		return errors.New("ZookeeperTimeout should have a duration > 0")
	}

	if cgc.CommitInterval <= 0 {
		return errors.New("CommitInterval should have a duration > 0")
	}

	if cgc.ChannelBufferSize < 0 {
		return errors.New("ChannelBufferSize should be >= 0.")
	}

	if cgc.KafkaClientConfig != nil {
		if err := cgc.KafkaClientConfig.Validate(); err != nil {
			return err
		}
	}

	if cgc.InitialOffsetMethod != sarama.OffsetMethodNewest && cgc.InitialOffsetMethod != sarama.OffsetMethodOldest {
		return errors.New("InitialOffsetMethod should OffsetMethodNewest or OffsetMethodOldest.")
	}

	if cgc.KafkaConsumerConfig != nil {
		if err := cgc.KafkaConsumerConfig.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type partitionOffsetTracker struct {
	l                   sync.Mutex
	highestOffsetSeen   int64
	lastCommittedOffset int64
}

func newPartitionOffsetTracker(lastCommittedOffset int64) *partitionOffsetTracker {
	return &partitionOffsetTracker{lastCommittedOffset: lastCommittedOffset}
}

func (pot *partitionOffsetTracker) MarkAsSeen(offset int64) {
	pot.l.Lock()
	defer pot.l.Unlock()
	if offset > pot.highestOffsetSeen {
		pot.highestOffsetSeen = offset
	} else {
		fmt.Println("Already marked as seen", offset)
	}
}

func (pot *partitionOffsetTracker) Commit(committer offsetCommitter) error {
	pot.l.Lock()
	defer pot.l.Unlock()

	if pot.highestOffsetSeen > pot.lastCommittedOffset {
		if err := committer(pot.highestOffsetSeen); err != nil {
			return err
		}
		pot.lastCommittedOffset = pot.highestOffsetSeen
	}
	return nil
}

type offsetCommitter func(int64) error
type topicOffsets map[int32]*partitionOffsetTracker

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
type ConsumerGroup struct {
	id, name string

	config *ConsumerGroupConfig

	client   *sarama.Client
	consumer *sarama.Consumer
	zk       *ZK

	wg             sync.WaitGroup
	singleShutdown sync.Once

	events  chan *sarama.ConsumerEvent
	acks    chan *sarama.ConsumerEvent
	stopper chan struct{}

	brokers   map[int]string
	consumers []string

	offsets     map[string]topicOffsets
	offsetsLock sync.RWMutex
}

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topics []string, zookeeper []string, config *ConsumerGroupConfig) (cg *ConsumerGroup, err error) {

	if name == "" {
		return nil, sarama.ConfigurationError("Empty consumergroup name")
	}

	if len(topics) == 0 {
		return nil, sarama.ConfigurationError("No topics provided")
	}

	if len(zookeeper) == 0 {
		return nil, errors.New("You need to provide at least one zookeeper node address!")
	}

	if config == nil {
		config = NewConsumerGroupConfig()
	}

	// Validate configuration
	if err = config.Validate(); err != nil {
		return
	}

	var zk *ZK
	if zk, err = NewZK(zookeeper, config.ZookeeperChroot, config.ZookeeperTimeout); err != nil {
		return
	}

	brokers, err := zk.Brokers()
	if err != nil {
		zk.Close()
		return
	}

	brokerList := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		brokerList = append(brokerList, broker)
	}

	var client *sarama.Client
	if client, err = sarama.NewClient(name, brokerList, config.KafkaClientConfig); err != nil {
		zk.Close()
		return
	}

	var consumer *sarama.Consumer
	if consumer, err = sarama.NewConsumer(client, config.KafkaConsumerConfig); err != nil {
		client.Close()
		zk.Close()
		return
	}

	var consumerID string
	consumerID, err = generateConsumerID()
	if err != nil {
		client.Close()
		zk.Close()
		return
	}

	cg = &ConsumerGroup{
		id:       consumerID,
		name:     name,
		config:   config,
		brokers:  brokers,
		client:   client,
		consumer: consumer,
		zk:       zk,
		events:   make(chan *sarama.ConsumerEvent, config.ChannelBufferSize),
		acks:     make(chan *sarama.ConsumerEvent, config.ChannelBufferSize),
		stopper:  make(chan struct{}),
		offsets:  make(map[string]topicOffsets),
	}

	// Register consumer group
	if err = zk.RegisterGroup(name); err != nil {
		cg.Logf("FAILED to register consumer group (%s)!\n")
		return
	}

	// Register itself with zookeeper
	if err = zk.RegisterConsumer(name, consumerID, topics); err != nil {
		cg.Logf("FAILED to register consumer instance (%s)!\n", cg.id)
		return
	} else {
		cg.Logf("Consumer instance registered (%s).", cg.id)
	}

	go cg.offsetCollector()
	go cg.offsetCommitter()
	go cg.topicListConsumer(topics)

	return
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Events() <-chan *sarama.ConsumerEvent {
	return cg.events
}

func (cg *ConsumerGroup) Closed() bool {
	return cg.id == ""
}

func (cg *ConsumerGroup) Close() error {
	shutdownError := AlreadyClosing
	cg.singleShutdown.Do(func() {
		defer cg.zk.Close()

		shutdownError = nil

		close(cg.stopper)
		cg.wg.Wait()

		if shutdownError = cg.zk.DeregisterConsumer(cg.name, cg.id); shutdownError != nil {
			cg.Logf("FAILED deregistering consumer: %s!\n", shutdownError)
		} else {
			cg.Logf("Deregistered consumer.\n")
		}

		if shutdownError = cg.client.Close(); shutdownError != nil {
			cg.Logf("FAILED closing the Sarama client: %s\n", shutdownError)
		}

		close(cg.events)
		cg.id = ""

	})

	return shutdownError
}

func (cg *ConsumerGroup) Logf(format string, args ...interface{}) {
	var identifier string
	if cg.id == "" {
		identifier = "(defunct)"
	} else {
		identifier = cg.id[len(cg.id)-12:]
	}
	sarama.Logger.Printf("[%s/%s] %s", cg.name, identifier, fmt.Sprintf(format, args...))
}

func (cg *ConsumerGroup) CommitUpto(event *sarama.ConsumerEvent) error {
	cg.acks <- event
	return nil
}

func (cg *ConsumerGroup) offsetCommitter() {
	defer cg.closeOnPanic()
	defer cg.commitOffsets()

	commitTicker := time.NewTicker(cg.config.CommitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-cg.stopper:
			return
		case <-commitTicker.C:
			cg.commitOffsets()
		}
	}
}

func (cg *ConsumerGroup) commitOffsets() {
	cg.Logf("Comitting offsets to ZK...\n")
	for topic, partitionOffsets := range cg.offsets {
		for partition, offsetTracker := range partitionOffsets {
			committer := func(offset int64) error {
				return cg.zk.Commit(cg.name, topic, partition, offset+1)
			}

			err := offsetTracker.Commit(committer)
			switch err {
			case nil:
				cg.Logf("Committed offset %d for %s:%d\n", offsetTracker.highestOffsetSeen, topic, partition)
			case AlreadyCommitted:
				// noop
			default:
				cg.Logf("Failed to commit offset %d for %s:%d\n", offsetTracker.highestOffsetSeen, topic, partition)
			}
		}
	}
}

func (cg *ConsumerGroup) offsetCollector() {
	defer cg.closeOnPanic()

	for {
		select {
		case <-cg.stopper:
			return

		case event := <-cg.acks:
			if event.Err != nil {
				// You should not ack errors, will ignore for now
				continue
			}
			cg.updateOffset(event)
		}
	}
}

func (cg *ConsumerGroup) updateOffset(event *sarama.ConsumerEvent) {
	cg.offsetsLock.RLock()
	defer cg.offsetsLock.RUnlock()
	cg.offsets[event.Topic][event.Partition].MarkAsSeen(event.Offset)
}

func (cg *ConsumerGroup) closeOnPanic() {
	if err := recover(); err != nil {
		cg.Logf("Error: %s\n", err)

		// Try to produce an error event on the channel so we can inform the consumer.
		// If that doesn't work, continue.
		errorEvent := &sarama.ConsumerEvent{Err: fmt.Errorf("%s", err)}
		select {
		case cg.events <- errorEvent:
		default:
		}

		// Now, close the consumer
		cg.Close()
	}
}

func (cg *ConsumerGroup) topicListConsumer(topics []string) {
	defer cg.closeOnPanic()

	for {
		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.zk.Consumers(cg.name)
		if err != nil {
			panic(err)
		}

		cg.consumers = consumers
		cg.Logf("Currently registered consumers: %d\n", len(cg.consumers))

		stopper := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.topicConsumer(topic, cg.events, stopper)
		}

		select {
		case <-cg.stopper:
			close(stopper)
			return

		case <-consumerChanges:
			cg.Logf("Triggering rebalance due to consumer list change.\n")
			close(stopper)
			cg.wg.Wait()
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, events chan<- *sarama.ConsumerEvent, stopper <-chan struct{}) {
	defer cg.closeOnPanic()
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	cg.Logf("Started topic consumer for %s\n", topic)

	cg.offsetsLock.Lock()
	if cg.offsets[topic] == nil {
		cg.offsets[topic] = make(topicOffsets)
	}
	cg.offsetsLock.Unlock()

	// Fetch a list of partition IDs
	partitions, err := cg.zk.Partitions(topic)
	if err != nil {
		panic(err)
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitions)
	myPartitions := dividedPartitions[cg.id]
	cg.Logf("Claiming %d of %d partitions for topic %s.", len(myPartitions), len(partitions), topic)

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, pid := range myPartitions {

		wg.Add(1)
		go cg.partitionConsumer(topic, pid.id, events, &wg, stopper)
	}

	wg.Wait()
	cg.Logf("Stopped topic consumer for %s\n", topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, events chan<- *sarama.ConsumerEvent, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer cg.closeOnPanic()
	defer wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	err := cg.zk.Claim(cg.name, topic, partition, cg.id)
	if err != nil {
		panic(err)
	}
	defer cg.zk.Release(cg.name, topic, partition, cg.id)

	nextOffset, err := cg.zk.Offset(cg.name, topic, partition)
	if err != nil {
		panic(err)
	}

	cg.offsetsLock.Lock()
	cg.offsets[topic][partition] = newPartitionOffsetTracker(nextOffset - 1)
	cg.offsetsLock.Unlock()

	config := sarama.NewPartitionConsumerConfig()
	if nextOffset > 0 {
		config.OffsetMethod = sarama.OffsetMethodManual
		config.OffsetValue = nextOffset
	} else {
		config.OffsetMethod = cg.config.InitialOffsetMethod
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	cg.Logf("Started partition consumer for %s:%d at offset %d.\n", topic, partition, nextOffset)

	partitionEvents := consumer.Events()

	err = nil
	var lastOffset int64 = -1 // aka unknown
partitionConsumerLoop:
	for {
		select {
		case event := <-partitionEvents:
			for {
				select {
				case events <- event:
					if event.Err == nil {
						lastOffset = event.Offset
					}
					continue partitionConsumerLoop

				case <-stopper:
					break partitionConsumerLoop
				}
			}

		case <-stopper:
			break partitionConsumerLoop
		}
	}

	cg.Logf("Stopping partition consumer for %s:%d at offset %d.\n", topic, partition, lastOffset)
}
