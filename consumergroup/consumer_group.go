package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	AlreadyClosed          = errors.New("Already closed consumer")
	FailedToClaimPartition = errors.New("Failed to claim partition for this consumer instance. Do you have a rogue consumer running?")
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
		CommitInterval:      10 * time.Second,
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

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
type ConsumerGroup struct {
	id, name string

	config *ConsumerGroupConfig

	client *sarama.Client
	zk     *ZK

	wg sync.WaitGroup

	events  chan *sarama.ConsumerEvent
	stopper chan struct{}

	brokers   map[int]string
	consumers []string
	offsets   map[string]map[int32]int64
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
		return
	}

	brokerList := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		brokerList = append(brokerList, broker)
	}

	var client *sarama.Client
	if client, err = sarama.NewClient(name, brokerList, config.KafkaClientConfig); err != nil {
		return
	}

	var consumerID string
	consumerID, err = generateConsumerID()
	if err != nil {
		return
	}

	cg = &ConsumerGroup{
		id:      consumerID,
		name:    name,
		config:  config,
		brokers: brokers,
		client:  client,
		zk:      zk,
		events:  make(chan *sarama.ConsumerEvent, config.ChannelBufferSize),
		stopper: make(chan struct{}),
		offsets: make(map[string]map[int32]int64),
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

func (cg *ConsumerGroup) Close() (err error) {
	if cg.Closed() {
		return AlreadyClosed
	}

	// Mark ConsumerGroup as closed BEFORE closing internal channels so other
	// go routines do not try to close ConsumerGroup again (thus causing
	// panic from closing channels twice).
	cg.id = ""

	defer cg.zk.Close()

	close(cg.stopper)
	cg.wg.Wait()

	if err = cg.client.Close(); err != nil {
		cg.Logf("FAILED closing the Sarama client!\n")
	}

	if err = cg.zk.DeregisterConsumer(cg.name, cg.id); err != nil {
		cg.Logf("FAILED deregistering consumer!\n")
	} else {
		cg.Logf("Deregistered consumer.\n")
	}

	close(cg.events)
	return
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

	config := sarama.NewConsumerConfig()
	part_config := sarama.NewPartitionConsumerConfig()
	if cg.config.KafkaConsumerConfig != nil {
		*config = *cg.config.KafkaConsumerConfig
	}

	if nextOffset > 0 {
		part_config.OffsetMethod = sarama.OffsetMethodManual
		part_config.OffsetValue = nextOffset
	} else {
		part_config.OffsetMethod = cg.config.InitialOffsetMethod
	}

    consumer, err := sarama.NewConsumer(cg.client, config)
	if err != nil {
		panic(err)
	}
	part_consumer, err := consumer.ConsumePartition(topic, partition, part_config)
	//part_consumer, err := sarama.NewConsumer(cg.client, topic, partition, cg.name, config)
	if err != nil {
		panic(err)
	}
	defer part_consumer.Close()

	cg.Logf("Started partition consumer for %s:%d at offset %d.\n", topic, partition, nextOffset)

	partitionEvents := part_consumer.Events()
	commitTicker := time.NewTicker(cg.config.CommitInterval)
	defer commitTicker.Stop()

	var lastCommittedOffset int64 = -1		// aka unknown

	err = nil
	var lastOffset int64 = -1				// aka unknown
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

		case <-commitTicker.C:
			if lastCommittedOffset < lastOffset {
				if err := cg.zk.Commit(cg.name, topic, partition, lastOffset+1); err != nil {
					cg.Logf("Failed to commit offset for %s:%d\n", topic, partition)
				} else {
					lastCommittedOffset = lastOffset
				}
			}

		case <-stopper:
			break partitionConsumerLoop
		}
	}

	if lastCommittedOffset < lastOffset {
		if err := cg.zk.Commit(cg.name, topic, partition, lastOffset+1); err != nil {
			cg.Logf("Failed to commit offset for %s:%d\n", topic, partition)
		} else {
			cg.Logf("Committed offset %d for %s:%d\n", lastOffset, topic, partition)
		}
	}

	cg.Logf("Stopping partition consumer for %s:%d at offset %d.\n", topic, partition, lastOffset)
}
