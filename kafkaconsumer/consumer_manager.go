package kafkaconsumer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
	"gopkg.in/tomb.v1"
)

// Consumer represents a consumer instance and is the main interface to work with as a consumer
// of this library.
type Consumer interface {
	// Interrups will initiate the shutdown procedure of the consumer, and return immediately.
	// When you are done using the consumer, you must either call Close or Interrupt to prevent leaking memory.
	Interrupt()

	// Closes will start the shutdown procedure for the consumer and wait for it to complete.
	// When you are done using the consumer, you must either call Close or Interrupt to prevent leaking memory.
	Close() error

	// Messages returns a channel that you can read to obtain messages from Kafka to process.
	// Every message that you receive from this channel should be sent to Ack after it has been processed.
	Messages() <-chan *sarama.ConsumerMessage

	// Error returns a channel that you can read to obtain errors that occur.
	Errors() <-chan error

	// Ack marks a message as processed, indicating that the message offset can be committed
	// for the message's partition by the offset manager. Note that the offset manager may decide
	// not to commit every offset immediately for efficiency reasons. Calling Close or Interrupt
	// will make sure that the last offset provided to this function will be flushed to storage.
	// You have to provide the messages in the same order as you received them from the Messages
	// channel.
	Ack(*sarama.ConsumerMessage)
}

// Join joins a Kafka consumer group, and returns a Consumer instance.
// - `group` is the name of the group this consumer instance will join . All instances that form
//   a consumer group should use the same name. A group name must be unique per Kafka cluster.
// - `subscription` is an object that describes what partitions the group wants to consume.
//   A single instance may end up consuming between zero of them, or all of them, or any number
//   in between. Every running instance in a group should use the same subscription; the behavior
//   is undefined if that is not the case.
// - `zookeeper` is the zookeeper connection string, e.g. "zk1:2181,zk2:2181,zk3:2181/chroot"
// - `config` specifies the configuration. If it is nil, a default configuration is used.
func Join(group string, subscription Subscription, zookeeper string, config *Config) (Consumer, error) {
	if group == "" {
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

	cm.group = cm.kz.Consumergroup(group)
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
		cm.logf("Consumer instance registered (%s).", cm.instance.ID)
	}

	// Discover the Kafka brokers
	brokers, err := cm.kz.BrokerList()
	if err != nil {
		cm.shutdown()
		return nil, err
	} else {
		cm.logf("Discovered Kafka cluster at %s", strings.Join(brokers, ","))
	}

	// Initialize sarama client
	if client, err := sarama.NewClient(brokers, config.Config); err != nil {
		cm.shutdown()
		return nil, err
	} else {
		cm.client = client
	}

	// Initialize sarama offset manager
	if offsetManager, err := sarama.NewOffsetManagerFromClient(group, cm.client); err != nil {
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

// consumerManager implements the Consumer interface, and manages the goroutine that
// is responsible for spawning and terminating partitionManagers.
type consumerManager struct {
	config       *Config
	subscription Subscription

	kz       *kazoo.Kazoo
	group    *kazoo.Consumergroup
	instance *kazoo.ConsumergroupInstance

	client        sarama.Client
	consumer      sarama.Consumer
	offsetManager sarama.OffsetManager

	t                 tomb.Tomb
	m                 sync.RWMutex
	partitionManagers map[string]*partitionManager

	messages chan *sarama.ConsumerMessage
	errors   chan error
}

func (cm *consumerManager) Messages() <-chan *sarama.ConsumerMessage {
	return cm.messages
}

func (cm *consumerManager) Errors() <-chan error {
	return cm.errors
}

func (cm *consumerManager) Interrupt() {
	cm.t.Kill(nil)
}

func (cm *consumerManager) Close() error {
	cm.Interrupt()
	return cm.t.Wait()
}

// Ack will dispatch a message to the right partitionManager's ack
// function, so it can be marked as processed.
func (cm *consumerManager) Ack(msg *sarama.ConsumerMessage) {
	cm.m.RLock()
	defer cm.m.RUnlock()

	partitionKey := fmt.Sprintf("%s/%d", msg.Topic, msg.Partition)
	partitionManager := cm.partitionManagers[partitionKey]
	if partitionManager == nil {
		cm.logf("ERROR: acked message %d for %s, but this partition is not managed by this consumer!", msg.Offset, partitionKey)
	} else {
		partitionManager.ack(msg.Offset)
	}
}

// run implements the main loop of the consumer manager.
// 1. Get partitions that the group subscribes to
// 2. Get the currently running instances
// 3. Distribute partitions over instances
// 4. Run partition consumers for the instances that are assigned to this instance
// 5. Watch zookeeper for changes in 1 & 2; start over when that happens.
func (cm *consumerManager) run() {
	defer cm.shutdown()

	for {
		partitions, partitionsChanged, err := cm.watchSubscription()
		if err != nil {
			cm.t.Kill(err)
			return
		}

		instances, instancesChanged, err := cm.watchConsumerInstances()
		if err != nil {
			cm.t.Kill(err)
			return
		}

		cm.logf("Currently, %d instances are registered, to consume %d partitions in total.", len(instances), len(partitions))

		var (
			partitionDistribution = distributePartitionsBetweenConsumers(instances, retrievePartitionLeaders(partitions))
			assignedPartitions    = make(map[string]*kazoo.Partition)
		)

		for _, partition := range partitionDistribution[cm.instance.ID] {
			assignedPartitions[partition.Key()] = partition
		}

		cm.managePartitionManagers(assignedPartitions)

		select {
		case <-cm.t.Dying():
			cm.logf("Interrupted, shutting down...")
			return

		case <-partitionsChanged:
			cm.logf("Woke up because the subscription reported a change in partitions.")

		case <-instancesChanged:
			cm.logf("Woke up because the list of running instances changed.")
		}
	}
}

// watchConsumerInstances retrieves the list of currently running consumer instances from Zookeeper,
// and sets a watch to be notified of changes to this list. It will retry for any error that may
// occur. If the consumer manager is interrupted, the error return value will be tomb.ErrDying. Any
// other error is non-recoverable.
func (cm *consumerManager) watchSubscription() (kazoo.PartitionList, <-chan zk.Event, error) {
	var (
		partitions        kazoo.PartitionList
		partitionsChanged <-chan zk.Event
		err               error
	)

	for {
		partitions, partitionsChanged, err = cm.subscription.WatchPartitions(cm.kz)
		if err != nil {
			cm.logf("Failed to watch subscription: %s. Trying again in 1 second...", err)
			select {
			case <-cm.t.Dying():
				return nil, nil, tomb.ErrDying
			case <-time.After(1 * time.Second):
				continue
			}
		}

		return partitions, partitionsChanged, nil
	}
}

// watchConsumerInstances retrieves the list of currently running consumer instances from Zookeeper,
// and sets a watch to be notified of changes to this list. It will retry for any error that may
// occur. If the consumer manager is interrupted, the error return value will be tomb.ErrDying. Any
// other error is non-recoverable.
func (cm *consumerManager) watchConsumerInstances() (kazoo.ConsumergroupInstanceList, <-chan zk.Event, error) {
	var (
		instances        kazoo.ConsumergroupInstanceList
		instancesChanged <-chan zk.Event
		err              error
	)

	for {
		instances, instancesChanged, err = cm.group.WatchInstances()
		if err != nil {
			cm.logf("Failed to watch consumer group instances: %s. Trying again in 1 second...", err)
			select {
			case <-cm.t.Dying():
				return nil, nil, tomb.ErrDying
			case <-time.After(1 * time.Second):
				continue
			}
		}

		return instances, instancesChanged, err
	}
}

// startPartitionManager starts a new partition manager in a a goroutine, and adds
// it to the partitionManagers map.
func (cm *consumerManager) startPartitionManager(partition *kazoo.Partition) {
	pm := &partitionManager{
		parent:             cm,
		partition:          partition,
		lastConsumedOffset: -1,
		processingDone:     make(chan struct{}),
	}

	cm.m.Lock()
	cm.partitionManagers[pm.partition.Key()] = pm
	cm.m.Unlock()

	go pm.run()
}

// startPartitionManager stops a running partition manager, and rmoves it
// from the partitionManagers map.
func (cm *consumerManager) stopPartitionManager(pm *partitionManager) {
	if err := pm.close(); err != nil {
		pm.logf("Failed to cleanly shut down consumer: %s", err)
	}

	cm.m.Lock()
	delete(cm.partitionManagers, pm.partition.Key())
	cm.m.Unlock()
}

// managePartitionManagers will compare the currently running partition managers to the list
// of partitions that is assigned to this consumer instance, and will stop and start partition
// managers as appropriate
func (cm *consumerManager) managePartitionManagers(assignedPartitions map[string]*kazoo.Partition) {
	var wg sync.WaitGroup

	cm.m.RLock()
	cm.logf("This instance is assigned to consume %d partitions, and is currently consuming %d partitions.", len(assignedPartitions), len(cm.partitionManagers))

	// Stop consumers for partitions that we were not already consuming
	for partitionKey, pm := range cm.partitionManagers {
		if _, ok := assignedPartitions[partitionKey]; !ok {
			wg.Add(1)
			go func(pm *partitionManager) {
				defer wg.Done()
				cm.stopPartitionManager(pm)
			}(pm)
		}
	}

	// Start consumers for partitions that we were not already consuming
	for partitionKey, partition := range assignedPartitions {
		if _, ok := cm.partitionManagers[partitionKey]; !ok {
			wg.Add(1)
			go func(partition *kazoo.Partition) {
				defer wg.Done()
				cm.startPartitionManager(partition)
			}(partition)
		}
	}

	cm.m.RUnlock()

	// Wait until all the interrupted partionManagers have shut down completely.
	wg.Wait()
}

// shutdown cleanly shuts down the consumer manager:
// 1. stop all partition managers
// 2. close connection to Kafka cluster
// 3. deregister this running instance in zookeeper
// 4. close connection to zookeeper.
// 5. close messages and errors channels.
func (cm *consumerManager) shutdown() {
	defer cm.t.Done()

	cm.managePartitionManagers(nil)

	if cm.consumer != nil {
		if err := cm.consumer.Close(); err != nil {
			cm.logf("Failed to close Kafka client: %s", err)
		}
	}

	if cm.client != nil {
		if err := cm.client.Close(); err != nil {
			cm.logf("Failed to close Kafka offset manager: %s", err)
		}
	}

	if cm.instance != nil {
		if err := cm.instance.Deregister(); err != nil {
			cm.logf("Failed to deregister consumer instance: %s", err)
		}
	}

	if cm.kz != nil {
		if err := cm.kz.Close(); err != nil {
			cm.logf("Failed to close Zookeeper connection: %s", err)
		}
	}

	close(cm.messages)
	close(cm.errors)
}

func (cm *consumerManager) shortID() string {
	if cm.instance == nil {
		return "(defunct)"
	} else {
		return cm.instance.ID[len(cm.instance.ID)-12:]
	}
}

func (cm *consumerManager) logf(format string, arguments ...interface{}) {
	Logger.Printf(fmt.Sprintf("[instance=%s] %s", cm.shortID(), format), arguments...)
}
