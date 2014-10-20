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
	// Leave this empty for to not set a chroot.
	ZookeeperChroot string

	KafkaClientConfig   *sarama.ClientConfig   // This will be passed to Sarama when creating a new sarama.Client
	KafkaConsumerConfig *sarama.ConsumerConfig // This will be passed to Sarama when creating a new sarama.Consumer

	ChannelBufferSize int
}

func NewConsumerGroupConfig() *ConsumerGroupConfig {
	return &ConsumerGroupConfig{
		ZookeeperTimeout:    1 * time.Second,
		KafkaClientConfig:   sarama.NewClientConfig(),
		KafkaConsumerConfig: sarama.NewConsumerConfig(),
	}
}

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
	sarama.Logger.Printf("Registering consumer %s for group %s\n", consumerID, name)
	if err = zk.RegisterConsumer(name, consumerID, topics); err != nil {
		return
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
	}

	go group.topicListConsumer(topics)

	return group, nil
}

func (cg *ConsumerGroup) Events() <-chan *sarama.ConsumerEvent {
	return cg.events
}

func (cg *ConsumerGroup) Close() (err error) {
	defer cg.zk.Close()
	close(cg.stopper)
	cg.wg.Wait()

	cg.client.Close()

	if err = cg.zk.DeregisterConsumer(cg.name, cg.id); err != nil {
		sarama.Logger.Printf("FAILED deregistering consumer %s for group %s\n", cg.id, cg.name)
		return err
	} else {
		sarama.Logger.Printf("Deregistering consumer %s for group %s\n", cg.id, cg.name)
	}

	close(cg.events)
	return
}

func (cg *ConsumerGroup) topicListConsumer(topics []string) {
	for {

		consumers, consumerChanges, err := cg.zk.Consumers(cg.name)
		if err != nil {
			panic(err)
		}

		cg.consumers = consumers
		sarama.Logger.Println("Currently registered consumers:", cg.consumers)

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
			sarama.Logger.Printf("[%s/%s] Triggering rebalance due to consumer list change.\n", cg.name, cg.id)
			close(stopper)
			cg.wg.Wait()
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, events chan<- *sarama.ConsumerEvent, stopper <-chan struct{}) {
	defer cg.wg.Done()

	sarama.Logger.Printf("[%s/%s] Started topic consumer for %s\n", cg.name, cg.id, topic)

	// Fetch a list of partition IDs
	partitions, err := cg.zk.Partitions(topic)
	if err != nil {
		panic(err)
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitions)
	myPartitions := dividedPartitions[cg.id]
	sarama.Logger.Printf("Topic %s has %d partitions in total, claiming %d", topic, len(partitions), len(myPartitions))

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, pid := range myPartitions {

		wg.Add(1)
		go cg.partitionConsumer(topic, pid.id, events, &wg, stopper)
	}

	wg.Wait()
	sarama.Logger.Printf("[%s/%s] Stopped topic consumer for %s\n", cg.name, cg.id, topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, events chan<- *sarama.ConsumerEvent, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	err := cg.zk.Claim(cg.name, topic, partition, cg.id)
	if err != nil {
		panic(err)
	}
	defer cg.zk.Release(cg.name, topic, partition, cg.id)

	consumer, err := sarama.NewConsumer(cg.client, topic, partition, cg.name, cg.config.KafkaConsumerConfig)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	sarama.Logger.Printf("[%s/%s] Started partition consumer for %s/%d\n", cg.name, cg.id, topic, partition)

	partitionEvents := consumer.Events()
partitionConsumerLoop:
	for {
		select {
		case event := <-partitionEvents:
			// sarama.Logger.Printf("[%s/%s] Event on partition consumer for %s/%d\n", cg.name, cg.id, topic, partition)
			events <- event
		case <-stopper:
			break partitionConsumerLoop
		}
	}

	sarama.Logger.Printf("[%s/%s] Stopping partition consumer for %s/%d\n", cg.name, cg.id, topic, partition)
}
