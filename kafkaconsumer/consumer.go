package kafkaconsumer

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

// Consumer represents a consumer instance and is the main interface to work with as a consumer
// of this library.
type Consumer interface {
	// Interrupts the consumer. This will initiate the shutdown procedure, but returns immediately.
	Interrupt()

	// Closes the consumer. This will start the shutdown procedure and wait for it to complete.
	Close() error

	// Returns a channel that you can read to obtain messages from Kafka to process.
	Messages() <-chan *sarama.ConsumerMessage

	// Returns a channel that you can read to obtain errors that occur.
	Errors() <-chan error

	// Returns a channel that you should feed messages to after you have processed them.
	// Processed() chan<- *sarama.ConsumerMessage

	Commit(*sarama.ConsumerMessage)
}

// Join joins a Kafka consumer group, and returns a Consumer instance.
// - `group` is the name of the group this consumer instance will join . All instances that form
//   a consumer group should use the same name. A group name must be unique per Kafka cluster.
// - `subscription` is an object that describes what partitions the group wants to consume.
//   A single instance may end up consuming between zero of them, or all of them, or any number
//   in between.
// - `zookeeper` is the zookeeper connection string, e.g. "zk1:2181,zk2:2181,zk3:2181/chroot"
// - `config` specifies the configuration. If it is nil, a default configuration is used.
func Join(name string, subscription Subscription, zookeeper string, config *Config) (Consumer, error) {
	if name == "" {
		return nil, sarama.ConfigurationError("a group name cannot be empty")
	}

	if config == nil {
		config = NewConfig()
	}

	var zkNodes []string
	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeper)

	if err := config.Validate(); err != nil {
		return nil, err
	}

	cm := &consumerManager{
		config:       config,
		subscription: subscription,

		partitionManagers: make(map[string]*partitionManager),
		messages:          make(chan *sarama.ConsumerMessage, config.ChannelBufferSize),
		errors:            make(chan error, config.ChannelBufferSize),
	}

	if kz, err := kazoo.NewKazoo(zkNodes, config.Zookeeper); err != nil {
		return nil, err
	} else {
		cm.kz = kz
	}

	cm.group = cm.kz.Consumergroup(name)
	cm.instance = cm.group.NewInstance()

	// Register the consumer group if it does not exist yet
	if exists, err := cm.group.Exists(); err != nil {
		cm.shutdown()
		return nil, err

	} else if !exists {
		if err := cm.group.Create(); err != nil {
			cm.shutdown()
			return nil, err
		}
	}

	// Register itself with zookeeper
	data, err := subscription.JSON()
	if err != nil {
		cm.shutdown()
		return nil, err
	}
	if err := cm.instance.RegisterWithSubscription(data); err != nil {
		cm.shutdown()
		return nil, err
	} else {
		sarama.Logger.Printf("Consumer instance registered (%s).", cm.instance.ID)
	}

	// Discover the Kafka brokers
	brokers, err := cm.kz.BrokerList()
	if err != nil {
		cm.shutdown()
		return nil, err
	} else {
		sarama.Logger.Printf("Discovered Kafka cluster at %s", strings.Join(brokers, ","))
	}

	// Initialize sarama client
	if client, err := sarama.NewClient(brokers, config.Config); err != nil {
		cm.shutdown()
		return nil, err
	} else {
		cm.client = client
	}

	// Initialize sarama offset manager
	if offsetManager, err := sarama.NewOffsetManagerFromClient(name, cm.client); err != nil {
		cm.shutdown()
		return nil, err
	} else {
		cm.offsetManager = offsetManager
	}

	// Initialize sarama consumer
	if consumer, err := sarama.NewConsumerFromClient(cm.client); err != nil {
		cm.shutdown()
		return nil, err
	} else {
		cm.consumer = consumer
	}

	// Start the manager goroutine
	go cm.run()

	return cm, nil
}
