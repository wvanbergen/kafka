package consumergroup

import (
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type ConsumerGroupConfig struct {
	// The Zookeeper read timeout
	ZookeeperTimeout time.Duration

	// Zookeeper chroot to use. Should not include a trailing slash.
	// Leave this empty when your Kafka install does not use a chroot.
	ZookeeperChroot string

	KafkaClientConfig   *sarama.ClientConfig   // This will be passed to Sarama when creating a new sarama.Client
	KafkaConsumerConfig *sarama.ConsumerConfig // This will be passed to Sarama when creating a new sarama.Consumer

	ChannelBufferSize int           // The buffer size of the channel for the messages coming from Kafka. Zero means no buffering.
	CommitInterval    time.Duration // The interval between which the prossed offsets are commited to Zookeeper.
}

func NewConsumerGroupConfig() *ConsumerGroupConfig {
	return &ConsumerGroupConfig{
		ZookeeperTimeout:    1 * time.Second,
		KafkaClientConfig:   sarama.NewClientConfig(),
		KafkaConsumerConfig: sarama.NewConsumerConfig(),
		ChannelBufferSize:   16,
		CommitInterval:      10 * time.Second,
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

	// Register consumer group
	if err = zk.RegisterGroup(name); err != nil {
		return
	}

	// Register itself with zookeeper
	if err = zk.RegisterConsumer(name, consumerID, topics); err != nil {
		sarama.Logger.Printf("[%s] FAILED to register consumer %s!\n", name, consumerID)
		return
	} else {
		sarama.Logger.Printf("[%s] Registered consumer %s.\n", name, consumerID)
	}

	group := &ConsumerGroup{
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

	go group.topicListConsumer(topics)

	return group, nil
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Events() <-chan *sarama.ConsumerEvent {
	return cg.events
}

func (cg *ConsumerGroup) Close() (err error) {
	defer cg.zk.Close()
	close(cg.stopper)
	cg.wg.Wait()

	cg.client.Close()

	if err = cg.zk.DeregisterConsumer(cg.name, cg.id); err != nil {
		sarama.Logger.Printf("[%s] FAILED deregistering consumer %s!\n", cg.name, cg.id)
		return err
	} else {
		sarama.Logger.Printf("[%s] Deregistered consumer %s.\n", cg.name, cg.id)
	}

	close(cg.events)
	return
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
			panic(err)
		}

		cg.consumers = consumers
		sarama.Logger.Printf("[%s] Currently registered consumers: %d\n", cg.name, len(cg.consumers))

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
			sarama.Logger.Printf("[%s] Triggering rebalance due to consumer list change.\n", cg.name)
			close(stopper)
			cg.wg.Wait()
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, events chan<- *sarama.ConsumerEvent, stopper <-chan struct{}) {
	defer cg.wg.Done()

	sarama.Logger.Printf("[%s] Started topic consumer for %s\n", cg.name, topic)

	// Fetch a list of partition IDs
	partitions, err := cg.zk.Partitions(topic)
	if err != nil {
		panic(err)
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitions)
	myPartitions := dividedPartitions[cg.id]
	sarama.Logger.Printf("[%s] Claiming %d of %d partitions for topic %s.", cg.name, len(myPartitions), len(partitions), topic)

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, pid := range myPartitions {

		wg.Add(1)
		go cg.partitionConsumer(topic, pid.id, events, &wg, stopper)
	}

	wg.Wait()
	sarama.Logger.Printf("[%s] Stopped topic consumer for %s\n", cg.name, topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, events chan<- *sarama.ConsumerEvent, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	err := cg.zk.Claim(cg.name, topic, partition, cg.id)
	if err != nil {
		panic(err)
	}
	defer cg.zk.Release(cg.name, topic, partition, cg.id)

	lastOffset, err := cg.zk.Offset(cg.name, topic, partition)
	if err != nil {
		panic(err)
	}

	if lastOffset > 0 {
		cg.config.KafkaConsumerConfig.OffsetMethod = sarama.OffsetMethodManual
		cg.config.KafkaConsumerConfig.OffsetValue = lastOffset + 1
	} else {
		cg.config.KafkaConsumerConfig.OffsetMethod = sarama.OffsetMethodOldest
	}

	consumer, err := sarama.NewConsumer(cg.client, topic, partition, cg.name, cg.config.KafkaConsumerConfig)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	sarama.Logger.Printf("[%s] Started partition consumer for %s:%d at offset %d.\n", cg.name, topic, partition, lastOffset)

	partitionEvents := consumer.Events()
	commitInterval := time.After(cg.config.CommitInterval)

partitionConsumerLoop:
	for {
		select {
		case event := <-partitionEvents:
			if event.Err != nil {
				panic(event.Err)
			}

			lastOffset = event.Offset
			events <- event

		case <-commitInterval:
			if err := cg.zk.Commit(cg.name, topic, partition, lastOffset); err != nil {
				sarama.Logger.Printf("[%s] Failed to commit offset for %s:%d\n", cg.name, topic, partition)
			}

		case <-stopper:
			break partitionConsumerLoop
		}
	}

	if err := cg.zk.Commit(cg.name, topic, partition, lastOffset); err != nil {
		sarama.Logger.Printf("[%s] Failed to commit offset for %s:%d\n", cg.name, topic, partition)
	}

	sarama.Logger.Printf("[%s] Stopping partition consumer for %s:%d at offset %d.\n", cg.name, topic, partition, lastOffset)
}
