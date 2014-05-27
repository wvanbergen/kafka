package consumergroup

import (
	"github.com/Shopify/sarama"
	"time"
)

// EventStream is an abstraction of a sarama.Consumer
type EventStream interface {
	Events() <-chan *sarama.ConsumerEvent
	Close() error
}

// EventBatch is a batch of events from a single topic/partition
type EventBatch struct {
	Topic     string
	Partition int32
	Events    []sarama.ConsumerEvent
}

// Returns true if starts with an OffsetOutOfRange error
func (b *EventBatch) offsetIsOutOfRange() bool {
	if b == nil || len(b.Events) < 1 {
		return false
	}

	err := b.Events[0].Err
	if err == nil {
		return false
	}

	kerr, ok := err.(sarama.KError)
	return ok && kerr == sarama.OffsetOutOfRange
}

// PartitionConsumer can consume a single partition of a single topic
type PartitionConsumer struct {
	stream    EventStream
	group     *ConsumerGroup
	topic     string
	partition int32
	offset    int64
}

// NewPartitionConsumer creates a new partition consumer instance
func NewPartitionConsumer(group *ConsumerGroup, partition int32) (*PartitionConsumer, error) {

	consumerConfig := *group.config.KafkaConsumerConfig

	lastSeenOffset, offsetErr := group.Offset(partition)
	if offsetErr != nil {
		return nil, offsetErr
	}

	if lastSeenOffset > 0 {
		sarama.Logger.Printf("[Partition consumer] Requesting to resume partition %d from offset %d\n", partition, lastSeenOffset)
		consumerConfig.OffsetMethod = sarama.OffsetMethodManual
		consumerConfig.OffsetValue = lastSeenOffset + 1
	} else {
		sarama.Logger.Printf("[Partition consumer] No committed offset for partition %d, starting from oldest offset.", partition)
		consumerConfig.OffsetMethod = sarama.OffsetMethodOldest
	}

	consumer, err := sarama.NewConsumer(group.client, group.topic, partition, group.name, &consumerConfig)
	if err != nil {
		return nil, err
	}

	return &PartitionConsumer{
		group:     group,
		topic:     group.topic,
		partition: partition,
		stream:    consumer,
	}, nil
}

// Fetch returns a batch of events
// WARNING: may return nil if not events are available
func (p *PartitionConsumer) Fetch(stream chan *sarama.ConsumerEvent, duration time.Duration) error {
	events := p.stream.Events()
	timeout := time.After(duration)

	for {
		select {
		case <-timeout:
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			} else if event.Err != nil {
				sarama.Logger.Println("[Partition consumer] ERROR:", event.Err)
				return event.Err
			}

			stream <- event
			if event.Err == nil && event.Offset > p.offset {
				p.offset = event.Offset
			}
		}
	}
}

// Close closes a partition consumer
func (p *PartitionConsumer) Close() error {
	return p.stream.Close()
}
