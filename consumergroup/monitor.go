package consumergroup

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
)

// Keeps track of the status of a consumergroup, i.e. how far the processing is behind
// on the events that are available in Kafka.
type Monitor struct {
	zookeeperConnection *ZK            // The zookeeper connection that gets the current offsets of the consumergroup.
	zookeeperPath       string         // The path in zookeeper on which to find the current offsets for a consumergroup
	ZookeeperChroot     string         // The chroot to use in zookeeper
	kafkaConnection     *sarama.Client // The kafka connection that gets the available offsets.
}

type TopicProcessingLag map[int32]int64                       // The number of messages behind latest in Kafka for every partition in a topic
type ConsumerGroupProcessingLag map[string]TopicProcessingLag // The number of messages behind latest in Kafka for every topic the consumergroup is consuming.

// Instantiates a new consumergroup monitor. Retuns the number of messages the consumergroup is behind
// the latest offset in Kafka for every topic/partition the consumergroup is consuming.
func NewMonitor(name string, consumergroup string, zookeeper []string, config *ConsumerGroupConfig) (*Monitor, error) {
	if config == nil {
		config = NewConsumerGroupConfig()
	}
	config.SaramaConfig.ClientID = name

	zkConn, err := NewZK(zookeeper, config.ZookeeperChroot, config.ZookeeperTimeout)
	if err != nil {
		return nil, err
	}

	brokers, err := zkConn.Brokers()
	if err != nil {
		return nil, err
	}

	brokerList := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		brokerList = append(brokerList, broker)
	}

	saramaClient, err := sarama.NewClient(brokerList, config.SaramaConfig)
	if err != nil {
		return nil, err
	}

	return &Monitor{
		zookeeperConnection: zkConn,
		zookeeperPath:       fmt.Sprintf("/consumers/%s/offsets", consumergroup),
		kafkaConnection:     saramaClient,
	}, nil
}

// Returns the processing lag of the consumergroup.
func (m *Monitor) ProcessingLag() (ConsumerGroupProcessingLag, error) {
	eventsBehindLatest := make(ConsumerGroupProcessingLag)

	topics, err := m.getTopics()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error getting topics: %s", err))
	}

	for _, topic := range topics {
		eventsBehindLatest[topic] = make(TopicProcessingLag)

		partitions, err := m.getPartitions(topic)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting partitions for %s: %s", topic, err))
		}

		for _, partition := range partitions {
			currentOffset, _, err := m.zookeeperConnection.Get(fmt.Sprintf("%s/%s/%d", m.zookeeperPath, topic, partition))
			if err != nil {
				return nil, err
			}

			currentOffsetInt, err := strconv.ParseInt(string(currentOffset), 10, 64)
			if err != nil {
				return nil, err
			}

			latestOffsetInt, err := m.kafkaConnection.GetOffset(topic, partition, sarama.LatestOffsets)
			if err != nil {
				return nil, err
			}

			eventsBehindLatest[topic][partition] = latestOffsetInt - currentOffsetInt
		}
	}
	return eventsBehindLatest, nil
}

func (m *Monitor) getTopics() ([]string, error) {
	topics, _, zkErr := m.zookeeperConnection.Children(m.zookeeperPath)
	if zkErr != nil {
		return nil, zkErr
	}

	return topics, nil
}

func (m *Monitor) getPartitions(topic string) ([]int32, error) {
	partitions, _, err := m.zookeeperConnection.Children(fmt.Sprintf("%s/%s", m.zookeeperPath, topic))
	if err != nil {
		return nil, err
	}

	if len(partitions) == 0 {
		return nil, errors.New("No committed partitions for consumer group on topic")
	}

	partitionsInt := make([]int32, 0, len(partitions))
	for _, partition := range partitions {
		partitionInt, err := strconv.ParseInt(string(partition), 10, 32)
		if err != nil {
			return nil, err
		}
		partitionsInt = append(partitionsInt, int32(partitionInt))
	}

	return partitionsInt, nil
}
