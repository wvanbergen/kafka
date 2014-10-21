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
	consumerGroupName      string
	zookeeper, kafkaTopics []string
)

func init() {
	consumerGroupName = "integration_test"
	zookeeper = []string{"localhost:2181"}
	kafkaTopics = []string{"single_partition", "multi_partition"}
}

func ExampleConsumerGroup() {
	consumer, consumerErr := JoinConsumerGroup(consumerGroupName, kafkaTopics, zookeeper, nil)
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

func setupZookeeper(t *testing.T, topic string, partitions int32) {
	client := saramaClient()
	defer client.Close()

	// Connect to zookeeper to commit the last seen offset.
	// This way we should only produce events that we produce ourselves in this test.
	zk, zkErr := NewZK(zookeeper, "", 1*time.Second)
	if zkErr != nil {
		t.Fatal(zkErr)
	}
	defer zk.Close()

	for partition := int32(0); partition < partitions; partitions++ {
		// Retrieve the offset that Sarama will use for the next message on the topic/partition.
		nextOffset, offsetErr := client.GetOffset(topic, partition, sarama.LatestOffsets)
		if offsetErr != nil {
			t.Fatal(offsetErr)
		} else {
			t.Logf("Next offset for %s:%d = %d", topic, partition, nextOffset)
		}

		initialOffset := nextOffset - 1

		if err := zk.Commit(consumerGroupName, topic, partition, initialOffset); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegrationBasicUsage(t *testing.T) {
	setupZookeeper(t, "single_partition", 1)
	setupZookeeper(t, "multi_partition", 2)

	// Produce 100 events that we will consume
	go produceEvents(t, "single_partition", 100)
	go produceEvents(t, "multi_partition", 200)

	consumer, err := JoinConsumerGroup(consumerGroupName, kafkaTopics, zookeeper, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var eventCount int64

	events := consumer.Events()
	for eventCount < 300 {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Reader timeout")

		case event, ok := <-events:

			if !ok {
				t.Fatal("Event stream closed prematurely")
			} else if event.Err != nil {
				t.Fatal(err)
			}

			t.Logf("Topic: %s, partition: %d, offset %d", event.Topic, event.Partition, event.Offset)
			eventCount += 1
		}
	}
	t.Logf("Successfully read %d messages, closing!", eventCount)
}
