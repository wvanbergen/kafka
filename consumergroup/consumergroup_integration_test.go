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

func ExampleConsumerGroup() {
	consumerGroupName := "my_consumer_group_name"
	kafkaTopic := "my_topic"
	zookeeper := []string{"localhost:2181"}

	consumer, consumerErr := JoinConsumerGroup(consumerGroupName, kafkaTopic, zookeeper, nil)
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

	stream := consumer.Stream()
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

func saramaClient() *sarama.Client {
	client, err := sarama.NewClient("test_helper", []string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func produceEvents(t *testing.T, topic string, amount int64) error {
	client := saramaClient()
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
		t.Logf("Produced message %d", i)
	}

	return nil
}

func TestIntegrationBasicUsage(t *testing.T) {
	consumerGroupName := "integration_test"
	kafkaTopic := "single_partition"
	zookeeper := []string{"localhost:2181"}

	// Retrieve the offset that Sarama will use for the next message on the topic/partition.
	client := saramaClient()
	nextOffset, offsetErr := client.GetOffset(kafkaTopic, 0, sarama.LatestOffsets)
	if offsetErr != nil {
		t.Fatal(offsetErr)
	} else {
		t.Logf("Next offset: %d", nextOffset)
	}

	//
	initialOffset := nextOffset - 1

	// Connect to zookeeper to commit the last seen offset.
	// This way we should only produce events that we produce ourselves in this test.
	zk, zkErr := NewZK(zookeeper, "", 1*time.Second)
	if zkErr != nil {
		t.Fatal(zkErr)
	}
	if err := zk.Commit(consumerGroupName, kafkaTopic, 0, initialOffset); err != nil {
		t.Fatal(err)
	}
	zk.Close()

	// Produce 100 events that we will consume
	eventBatchSize := int64(100)
	go produceEvents(t, kafkaTopic, eventBatchSize)

	consumer, consumerErr := JoinConsumerGroup(consumerGroupName, kafkaTopic, zookeeper, nil)
	if consumerErr != nil {
		t.Fatal(consumerErr)
	}
	defer consumer.Close()

	var eventCount int64

	stream := consumer.Stream()
	for eventCount < eventBatchSize {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Reader timeout")

		case event, ok := <-stream:

			if !ok {
				t.Fatal("Event stream closed prematurely")
			}

			eventCount += 1
			if initialOffset+eventCount != event.Offset {
				t.Fatalf("Unexpected offset: %d. Expected %d\n", event.Offset, initialOffset+eventCount)
			} else {
				t.Logf("Consumed message %d with offset %d", eventCount, event.Offset)
			}

			expectedMessage := fmt.Sprintf("testing %d", eventCount)
			if string(event.Value) != expectedMessage {
				t.Fatalf("Unexpected message %d: %s / %s", eventCount, string(event.Value), expectedMessage)
			}
		}
	}
	t.Logf("Successfully read %d messages, closing!", eventBatchSize)
}
