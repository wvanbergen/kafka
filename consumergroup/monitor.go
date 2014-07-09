package consumergroup

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
)

type Monitor struct {
	ConsumerGroup       string
	zookeeperConnection *ZK
	kafkaConnection     *sarama.Client
}

func NewMonitor(name string, consumergroup string, zookeeper []string) (*Monitor, error) {
	config := NewConsumerGroupConfig()

	zkConn, zkErr := NewZK(zookeeper, config.ZookeeperTimeout)
	if zkErr != nil {
		return nil, zkErr
	}

	kafkaBrokers, brokersErr := zkConn.Brokers()
	if brokersErr != nil {
		return nil, brokersErr
	}

	saramaClient, saramaErr := sarama.NewClient(name, kafkaBrokers, config.KafkaClientConfig)
	if saramaErr != nil {
		return nil, saramaErr
	}

	return &Monitor{
		zookeeperConnection: zkConn,
		kafkaConnection:     saramaClient,
		ConsumerGroup:       consumergroup,
	}, nil
}

func (m *Monitor) Check() (map[string]map[int32]int64, error) {
	eventsBehindLatest := make(map[string]map[int32]int64)

	topics, err := m.getTopics()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error getting topics: %s", err))
	}

	for _, topic := range topics {
		eventsBehindLatest[topic] = make(map[int32]int64)

		partitions, err := m.getPartitions(topic)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting partitions for %s: %s", topic, err))
		}

		for _, partition := range partitions {
			currentOffset, _, zkErr := m.zookeeperConnection.Get(fmt.Sprintf("/consumers/%s/offsets/%s/%d", m.ConsumerGroup, topic, partition))
			if zkErr != nil {
				return nil, errors.New(fmt.Sprintf("Error getting consumer group offsets for %s/%d: %s", topic, partition, zkErr))
			}

			currentOffsetInt, err := strconv.ParseInt(string(currentOffset), 10, 64)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Error converting current offset to integer: %s", err))
			}

			latestOffsetInt, saramaErr := m.kafkaConnection.GetOffset(topic, partition, sarama.LatestOffsets)
			if saramaErr != nil {
				return nil, errors.New(fmt.Sprintf("Error converting latest offset to integer: %s", err))
			}

			eventsBehindLatest[topic][partition] = latestOffsetInt - currentOffsetInt - 1
		}
	}
	return eventsBehindLatest, nil
}

func (m *Monitor) getTopics() ([]string, error) {
	topics, _, zkErr := m.zookeeperConnection.Children(fmt.Sprintf("/consumers/%s/offsets", m.ConsumerGroup))
	if zkErr != nil {
		return nil, zkErr
	}

	return topics, nil
}

func (m *Monitor) getPartitions(topic string) ([]int32, error) {
	partitions, _, zkErr := m.zookeeperConnection.Children(fmt.Sprintf("/consumers/%s/offsets/%s", m.ConsumerGroup, topic))
	if zkErr != nil {
		return nil, zkErr
	}

	if len(partitions) == 0 {
		return nil, errors.New("No committed partitions for consumer group on topic")
	}

	partitionsInt := make([]int32, 0)
	for _, partition := range partitions {
		partitionInt, err := strconv.ParseInt(string(partition), 10, 32)
		if err != nil {
			return nil, err
		}
		partitionsInt = append(partitionsInt, int32(partitionInt))
	}

	return partitionsInt, nil
}
