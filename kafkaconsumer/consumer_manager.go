package kafkaconsumer

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
	"gopkg.in/tomb.v1"
)

type consumerManager struct {
	config       *Config
	subscription Subscription

	kz       *kazoo.Kazoo
	group    *kazoo.Consumergroup
	instance *kazoo.ConsumergroupInstance

	client        sarama.Client
	consumer      sarama.Consumer
	offsetManager sarama.OffsetManager

	t                 *tomb.Tomb
	partitionManagers map[string]*partitionManager

	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cm *consumerManager) Messages() <-chan *sarama.ConsumerMessage {
	return cm.messages
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cm *consumerManager) Errors() <-chan *sarama.ConsumerError {
	return cm.errors
}

func (cm *consumerManager) Interrupt() {
	cm.t.Kill(nil)
}

func (cm *consumerManager) Close() error {
	cm.Interrupt()
	return cm.t.Wait()
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
		partitions, partitionsChanged, err := cm.WatchSubscription()
		if err != nil {
			cm.t.Kill(err)
			return
		}

		instances, instancesChanged, err := cm.watchConsumerInstances()
		if err != nil {
			cm.t.Kill(err)
			return
		}

		sarama.Logger.Printf("Currently, %d instances are registered, to consume %d partitions in total.", len(instances), len(partitions))

		var (
			partitionDistribution = distributePartitionsBetweenConsumers(instances, retrievePartitionLeaders(partitions))
			assignedPartitions    = make(map[string]*kazoo.Partition)
		)

		for _, partition := range partitionDistribution[cm.instance.ID] {
			assignedPartitions[partition.Key()] = partition
		}

		sarama.Logger.Printf("This instance is assigned to consume %d partitions, and is currently consuming %d partitions.", len(assignedPartitions), len(cm.partitionManagers))
		cm.managePartitionManagers(assignedPartitions)

		select {
		case <-cm.t.Dying():
			sarama.Logger.Printf("Consumer was interrupted, shutting down...")
			return

		case <-partitionsChanged:
			sarama.Logger.Printf("Woke up because the subscription reported a change in partitions.")

		case <-instancesChanged:
			sarama.Logger.Printf("Woke up because the list of running instances changed.")
		}
	}
}

func (cm *consumerManager) WatchSubscription() (kazoo.PartitionList, <-chan zk.Event, error) {
	var (
		partitions        kazoo.PartitionList
		partitionsChanged <-chan zk.Event
		err               error
	)

	for {
		select {
		case <-cm.t.Dying():
			return nil, nil, tomb.ErrDying
		default:
		}

		partitions, partitionsChanged, err = cm.subscription.WatchPartitions(cm.kz)
		if err != nil {
			sarama.Logger.Println("Failed to watch subscription:", err)
			sarama.Logger.Println("Trying again in 1 second...")
			time.Sleep(1 * time.Second)
			continue
		}

		return partitions, partitionsChanged, nil
	}
}

func (cm *consumerManager) watchConsumerInstances() (kazoo.ConsumergroupInstanceList, <-chan zk.Event, error) {
	var (
		instances        kazoo.ConsumergroupInstanceList
		instancesChanged <-chan zk.Event
		err              error
	)

	for {
		select {
		case <-cm.t.Dying():
			return nil, nil, tomb.ErrDying
		default:
		}

		instances, instancesChanged, err = cm.group.WatchInstances()
		if err != nil {
			sarama.Logger.Println("Failed to watch consumer group instances:", err)
			sarama.Logger.Println("Trying again in 1 second...")
			time.Sleep(1 * time.Second)
			continue
		}

		return instances, instancesChanged, err
	}
}

// startPartitionManager starts a new partition manager in a a goroutine.
func (cm *consumerManager) startPartitionManager(partition *kazoo.Partition) *partitionManager {
	pc := &partitionManager{
		parent:    cm,
		partition: partition,
		t:         &tomb.Tomb{},
	}

	go pc.run()

	return pc
}

// managePartitionManagers will compare the currently running partition managers to the list
// of partitions that is assigned to this consumer instance, and will stop and start partition
// managers as appropriate
func (cm *consumerManager) managePartitionManagers(assignedPartitions map[string]*kazoo.Partition) {
	var wg sync.WaitGroup

	// Stop consumers for partitions that we were not already consuming
	for partitionKey, pm := range cm.partitionManagers {
		if _, ok := assignedPartitions[partitionKey]; !ok {
			wg.Add(1)
			go func(pm *partitionManager) {
				defer wg.Done()
				if err := pm.close(); err != nil {
					sarama.Logger.Printf("Failed to cleanly shut down consumer for %s: %s", pm.partition.Key(), err)
				}
			}(pm)
			delete(cm.partitionManagers, partitionKey)
		}
	}

	// Start consumers for partitions that we were not already consuming
	for partitionKey, partition := range assignedPartitions {
		if _, ok := cm.partitionManagers[partitionKey]; !ok {
			cm.partitionManagers[partitionKey] = cm.startPartitionManager(partition)
		}
	}

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
			sarama.Logger.Print("Failed to close Kafka client:", err)
		}
	}

	if cm.client != nil {
		if err := cm.client.Close(); err != nil {
			sarama.Logger.Print("Failed to close Kafka offset manager:", err)
		}
	}

	if cm.instance != nil {
		if err := cm.instance.Deregister(); err != nil {
			sarama.Logger.Print("Failed to deregister consumer instance:", err)
		}
	}

	if cm.kz != nil {
		if err := cm.kz.Close(); err != nil {
			sarama.Logger.Print("Failed to close Zookeeper connection:", err)
		}
	}

	close(cm.messages)
	close(cm.errors)
}
