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
)

type ConsumerGroupConfig struct {
	// The Zookeeper read timeout. Defaults to 1 second
	ZookeeperTimeout time.Duration

	// Zookeeper chroot to use. Should not include a trailing slash.
	// Leave this empty when your Kafka install does not use a chroot.
	ZookeeperChroot string

	SaramaConfig *sarama.Config // This will be passed to Sarama when creating a new consumer

	ChannelBufferSize  int           // The buffer size of the channel for the messages coming from Kafka. Zero means no buffering. Default is 256.
	CommitInterval     time.Duration // The interval between which the prossed offsets are commited to Zookeeper.
	InitialOffset      int64         // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetMethodOldest (default) or sarama.OffsetMethodNewest.
	FinalOffsetTimeout time.Duration // Time to wait for all the offsets for a partition to be processed after stopping to consume from it. Defaults to 1 minute.
}

func NewConsumerGroupConfig() *ConsumerGroupConfig {
	return &ConsumerGroupConfig{
		ZookeeperTimeout:   1 * time.Second,
		ChannelBufferSize:  256,
		CommitInterval:     10 * time.Second,
		InitialOffset:      sarama.OffsetOldest,
		FinalOffsetTimeout: 60 * time.Second,
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

	if cgc.SaramaConfig != nil {
		if err := cgc.SaramaConfig.Validate(); err != nil {
			return err
		}
	}

	if cgc.InitialOffset != sarama.OffsetOldest && cgc.InitialOffset != sarama.OffsetNewest {
		return errors.New("InitialOffset should OffsetMethodNewest or OffsetMethodOldest.")
	}

	return nil
}

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
type ConsumerGroup struct {
	id, name string

	config *ConsumerGroupConfig

	consumer *sarama.Consumer
	zk       *ZK

	wg             sync.WaitGroup
	singleShutdown sync.Once

	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	stopper  chan struct{}

	brokers   map[int]string
	consumers []string

	offsetManager OffsetManager
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
	if config.SaramaConfig == nil {
		config.SaramaConfig = sarama.NewConfig()
	}
	config.SaramaConfig.ClientID = name

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

	var consumer *sarama.Consumer
	if consumer, err = sarama.NewConsumer(brokerList, config.SaramaConfig); err != nil {
		zk.Close()
		return
	}

	var consumerID string
	consumerID, err = generateConsumerID()
	if err != nil {
		consumer.Close()
		zk.Close()
		return
	}

	cg = &ConsumerGroup{
		id:       consumerID,
		name:     name,
		config:   config,
		brokers:  brokers,
		consumer: consumer,
		zk:       zk,
		messages: make(chan *sarama.ConsumerMessage, config.ChannelBufferSize),
		errors:   make(chan *sarama.ConsumerError, config.ChannelBufferSize),
		stopper:  make(chan struct{}),
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

	offsetConfig := OffsetManagerConfig{CommitInterval: config.CommitInterval}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)

	go cg.topicListConsumer(topics)

	return
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	return cg.messages
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Errors() <-chan *sarama.ConsumerError {
	return cg.errors
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

		if err := cg.offsetManager.Close(); err != nil {
			cg.Logf("FAILED closing the offset manager: %s!\n", err)
		}

		if shutdownError = cg.zk.DeregisterConsumer(cg.name, cg.id); shutdownError != nil {
			cg.Logf("FAILED deregistering consumer instance: %s!\n", shutdownError)
		} else {
			cg.Logf("Deregistered consumer instance %s.\n", cg.id)
		}

		if shutdownError = cg.consumer.Close(); shutdownError != nil {
			cg.Logf("FAILED closing the Sarama client: %s\n", shutdownError)
		}

		close(cg.messages)
		close(cg.errors)
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

func (cg *ConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {
	cg.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
	return nil
}

func (cg *ConsumerGroup) topicListConsumer(topics []string) {
	for {
		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.zk.Consumers(cg.name)
		if err != nil {
			cg.Logf("FAILED to get list of registered consumer instances: %s\n", err)
			return
		}

		cg.consumers = consumers
		cg.Logf("Currently registered consumers: %d\n", len(cg.consumers))

		stopper := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.topicConsumer(topic, cg.messages, cg.errors, stopper)
		}

		select {
		case <-cg.stopper:
			close(stopper)
			return

		case <-consumerChanges:
			cg.Logf("Triggering rebalance due to consumer list change\n")
			close(stopper)
			cg.wg.Wait()
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, stopper <-chan struct{}) {
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	cg.Logf("%s :: Started topic consumer\n", topic)

	// Fetch a list of partition IDs
	partitions, err := cg.zk.Partitions(topic)
	if err != nil {
		cg.Logf("%s :: FAILED to get list of partitions: %s\n", topic, err)
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitions)
	myPartitions := dividedPartitions[cg.id]
	cg.Logf("%s :: Claiming %d of %d partitions", topic, len(myPartitions), len(partitions))

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, pid := range myPartitions {

		wg.Add(1)
		go cg.partitionConsumer(topic, pid.id, messages, errors, &wg, stopper)
	}

	wg.Wait()
	cg.Logf("%s :: Stopped topic consumer\n", topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	err := cg.zk.Claim(cg.name, topic, partition, cg.id)
	if err != nil {
		cg.Logf("%s/%d :: FAILED to claim the partition: %s\n", topic, partition, err)
		return
	}
	defer cg.zk.Release(cg.name, topic, partition, cg.id)

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)
	if err != nil {
		cg.Logf("%s/%d :: FAILED to determine initial offset: %s\n", topic, partition, err)
		return
	}

	if nextOffset > 0 {
		cg.Logf("%s/%d :: Partition consumer starting at offset %d.\n", topic, partition, nextOffset)
	} else {
		nextOffset = cg.config.InitialOffset
		if nextOffset == sarama.OffsetOldest {
			cg.Logf("%s/%d :: Partition consumer starting at the oldest available offset.\n", topic, partition)
		} else if nextOffset == sarama.OffsetNewest {
			cg.Logf("%s/%d :: Partition consumer listening for new messages only.\n", topic, partition)
		}
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, nextOffset)
	if err != nil {
		cg.Logf("%s/%d :: FAILED to start partition consumer: %s\n", topic, partition, err)
		return
	}
	defer consumer.Close()

	err = nil
	var lastOffset int64 = -1 // aka unknown
partitionConsumerLoop:
	for {
		select {
		case <-stopper:
			break partitionConsumerLoop

		case err := <-consumer.Errors():
			for {
				select {
				case errors <- err:
					continue partitionConsumerLoop

				case <-stopper:
					break partitionConsumerLoop
				}
			}

		case message := <-consumer.Messages():
			for {
				select {
				case <-stopper:
					break partitionConsumerLoop

				case messages <- message:
					lastOffset = message.Offset
					continue partitionConsumerLoop
				}
			}
		}
	}

	cg.Logf("%s/%d :: Stopping partition consumer at offset %d\n", topic, partition, lastOffset)
	if err := cg.offsetManager.FinalizePartition(topic, partition, lastOffset, cg.config.FinalOffsetTimeout); err != nil {
		cg.Logf("%s/%d :: %s\n", topic, partition, err)
	}
}
