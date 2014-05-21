package kafkaconsumer

import (
	"fmt"
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

	lastSeenOffset, offsetErr := group.Offset(partition)
	if offsetErr != nil {
		return nil, offsetErr
	}

	p := &PartitionConsumer{
		group:     group,
		topic:     group.topic,
		partition: partition,
	}

	if err := p.setSaramaConsumer(lastSeenOffset); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *PartitionConsumer) setSaramaConsumer(lastSeenOffset int64) error {
	config := sarama.ConsumerConfig{
		DefaultFetchSize: p.group.config.DefaultFetchSize,
		EventBufferSize:  p.group.config.EventBufferSize,
		MaxMessageSize:   p.group.config.MaxMessageSize,
		MaxWaitTime:      p.group.config.MaxWaitTime,
		MinFetchSize:     p.group.config.MinFetchSize,
		OffsetMethod:     sarama.OffsetMethodOldest,
	}

	if lastSeenOffset > 0 {
		fmt.Printf("Requesting to resume from offset %d\n", lastSeenOffset)
		config.OffsetMethod = sarama.OffsetMethodManual
		config.OffsetValue = lastSeenOffset + 1
	} else {
		fmt.Printf("Starting from offset 0\n")
	}

	consumer, err := sarama.NewConsumer(p.group.client, p.group.topic, p.partition, p.group.name, &config)
	if err != nil {
		return err
	}

	p.stream = consumer
	return nil
}

// Fetch returns a batch of events
// WARNING: may return nil if not events are available
func (p *PartitionConsumer) Fetch(stream chan *Event, duration time.Duration) error {
	events := p.stream.Events()
	timeout := time.After(duration)

	for {
		select {
		case <-timeout:
			return nil
		case event, ok := <-events:
			if !ok {
				fmt.Println("events channel was closed")
				return fmt.Errorf("events channel was closed")
			} else if event.Err == sarama.OffsetOutOfRange {
				p.stream.Close()
				if err := p.setSaramaConsumer(0); err != nil {
					return err
				}

				return p.Fetch(stream, duration)
			} else if event.Err != nil {
				fmt.Println("Fail", event.Err)
				return event.Err
			}

			stream <- &Event{ConsumerEvent: *event, Topic: p.topic, Partition: p.partition}
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
