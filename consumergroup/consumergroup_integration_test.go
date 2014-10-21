package consumergroup

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

var (
	zookeeper []string = []string{"localhost:2181"}
)

////////////////////////////////////////////////////////////////////
// Examples
////////////////////////////////////////////////////////////////////

func ExampleConsumerGroup() {
	consumer, consumerErr := JoinConsumerGroup(
		"ExampleConsumerGroup",
		[]string{"single_partition", "multi_partition"},
		[]string{"localhost:2181"},
		nil)

	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		consumer.Close()
	}()

	eventCount := 0

	stream := consumer.Events()
	for {
		event, ok := <-stream
		if !ok {
			break
		}

		// Process event
		log.Println(string(event.Value))

		eventCount += 1
	}

	log.Printf("Processed %d events.", eventCount)
}

////////////////////////////////////////////////////////////////////
// Integration tests
////////////////////////////////////////////////////////////////////

func TestIntegrationMultipleTopicsSingleConsumer(t *testing.T) {
	consumerGroup := "TestIntegrationMultipleTopicsSingleConsumer"
	setupZookeeper(t, consumerGroup, "single_partition", 1)
	setupZookeeper(t, consumerGroup, "multi_partition", 2)

	// Produce 100 events that we will consume
	go produceEvents(t, consumerGroup, "single_partition", 100)
	go produceEvents(t, consumerGroup, "multi_partition", 200)

	consumer, err := JoinConsumerGroup(consumerGroup, []string{"single_partition", "multi_partition"}, zookeeper, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var offsets = make(OffsetMap)
	assertEvents(t, consumer.Events(), 300, offsets)
}

func TestIntegrationSingleTopicParallelConsumers(t *testing.T) {
	consumerGroup := "TestIntegrationSingleTopicParallelConsumers"
	setupZookeeper(t, consumerGroup, "multi_partition", 2)
	go produceEvents(t, consumerGroup, "multi_partition", 200)

	consumer1, err := JoinConsumerGroup(consumerGroup, []string{"multi_partition"}, zookeeper, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer1.Close()

	consumer2, err := JoinConsumerGroup(consumerGroup, []string{"multi_partition"}, zookeeper, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer2.Close()

	var eventCount1, eventCount2 int64
	offsets := make(map[int32]int64)

	events1 := consumer1.Events()
	events2 := consumer2.Events()

	handleEvent := func(event *sarama.ConsumerEvent, ok bool) {
		if !ok {
			t.Fatal("Event stream closed prematurely")
		} else if event.Err != nil {
			t.Fatal(event.Err)
		}

		if offsets[event.Partition] != 0 && offsets[event.Partition]+1 != event.Offset {
			t.Fatalf("Unecpected offset on partition %d. Expected %d, got %d.", event.Partition, offsets[event.Partition]+1, event.Offset)
		}

		offsets[event.Partition] = event.Offset
	}

	for eventCount1+eventCount2 < 200 {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Reader timeout")

		case event1, ok1 := <-events1:
			handleEvent(event1, ok1)
			eventCount1 += 1

		case event2, ok2 := <-events2:
			handleEvent(event2, ok2)
			eventCount2 += 1
		}
	}

	if eventCount1 == 0 || eventCount2 == 0 {
		t.Error("Expected events to be consumed by both consumers!")
	} else {
		t.Logf("Successfully read %d and %d messages, closing!", eventCount1, eventCount2)
	}
}

func TestSingleTopicSequentialConsumer(t *testing.T) {
	sarama.Logger = log.New(os.Stdout, "", log.LstdFlags)

	consumerGroup := "TestSingleTopicSequentialConsumer"
	setupZookeeper(t, consumerGroup, "single_partition", 1)
	go produceEvents(t, consumerGroup, "single_partition", 20)

	offsets := make(OffsetMap)

	// If the channel is buffered, the consumer will enqueue more events in the channel,
	// which assertEvents will simply skip. When consumer 2 starts it will skip a bunch of
	// events because of this. Transactikonal processing will fix this.
	config := NewConsumerGroupConfig()
	config.ChannelBufferSize = 0

	sarama.Logger.Println("Start consumer 1")
	consumer1, err := JoinConsumerGroup(consumerGroup, []string{"single_partition"}, zookeeper, config)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer1.Close()

	assertEvents(t, consumer1.Events(), 10, offsets)
	sarama.Logger.Println("Closing consumer 1...")
	consumer1.Close()

	sarama.Logger.Println("Consumer 1 all done")
	sarama.Logger.Println("Start consumer 2")

	consumer2, err := JoinConsumerGroup(consumerGroup, []string{"single_partition"}, zookeeper, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer2.Close()

	assertEvents(t, consumer2.Events(), 10, offsets)
	consumer2.Close()

	sarama.Logger.Println("Consumer 2 all done")
}

////////////////////////////////////////////////////////////////////
// Helper functions and types
////////////////////////////////////////////////////////////////////

type OffsetMap map[string]map[int32]int64

func assertEvents(t *testing.T, stream <-chan *sarama.ConsumerEvent, count int64, offsets OffsetMap) {
	var processed int64
	for processed < count {
		select {
		case <-time.After(5 * time.Second):
			t.Fatalf("Reader timeout after %d events!", processed)

		case event, ok := <-stream:
			if !ok {
				t.Fatal("Event stream closed prematurely")
			} else if event.Err != nil {
				t.Fatal(event.Err)
			}

			if offsets != nil {
				if offsets[event.Topic] == nil {
					offsets[event.Topic] = make(map[int32]int64)
				}
				if offsets[event.Topic][event.Partition] != 0 && offsets[event.Topic][event.Partition]+1 != event.Offset {
					sarama.Logger.Printf("Unexpected offset on %s:%d. Expected %d, got %d.", event.Topic, event.Partition, offsets[event.Topic][event.Partition]+1, event.Offset)
					t.Fatalf("Unexpected offset on %s:%d. Expected %d, got %d.", event.Topic, event.Partition, offsets[event.Topic][event.Partition]+1, event.Offset)
				}

				processed += 1

				sarama.Logger.Printf("Offset %d (processed %d of %d)\n", event.Offset, processed, count)
				offsets[event.Topic][event.Partition] = event.Offset
			}

		}
	}
	t.Logf("Successfully asserted %d events.", count)
}

func saramaClient(name string) *sarama.Client {
	client, err := sarama.NewClient(name, []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func produceEvents(t *testing.T, consumerGroup string, topic string, amount int64) error {
	client := saramaClient(fmt.Sprintf("%s-%s", consumerGroup, "producer"))
	defer client.Close()

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := int64(1); i <= amount; i++ {
		err = producer.SendMessage(topic, nil, sarama.StringEncoder(fmt.Sprintf("testing %d", i)))

		if err != nil {
			return err
		}
	}

	return nil
}

func setupZookeeper(t *testing.T, consumerGroup string, topic string, partitions int32) {
	client := saramaClient(fmt.Sprintf("%s-%s", consumerGroup, "setup"))
	defer client.Close()

	// Connect to zookeeper to commit the last seen offset.
	// This way we should only produce events that we produce ourselves in this test.
	zk, zkErr := NewZK(zookeeper, "", 1*time.Second)
	if zkErr != nil {
		t.Fatal(zkErr)
	}
	defer zk.Close()

	for partition := int32(0); partition < partitions; partition++ {
		// Retrieve the offset that Sarama will use for the next message on the topic/partition.
		nextOffset, offsetErr := client.GetOffset(topic, partition, sarama.LatestOffsets)
		if offsetErr != nil {
			t.Fatal(offsetErr)
		} else {
			t.Logf("Next offset for %s:%d = %d", topic, partition, nextOffset)
		}

		initialOffset := nextOffset - 1

		if err := zk.Commit(consumerGroup, topic, partition, initialOffset); err != nil {
			t.Fatal(err)
		}
	}
}
