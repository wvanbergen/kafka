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

func produceEvents(topic string, amount int) error {
	client, err := sarama.NewClient("test_helper", []string{"localhost:9092"}, nil)
	if err != nil {
		return err
	}
	defer client.Close()

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 1; i <= amount; i++ {
		err = producer.SendMessage(topic, nil, sarama.StringEncoder(fmt.Sprintf("testing %d", i)))

		if err != nil {
			return err
		}
		log.Printf("Produced message %d", i)
	}

	return nil
}

func TestIntegrationBasicUsage(t *testing.T) {
	consumerGroupName := "integration_test"
	kafkaTopic := "single_partition"
	zookeeper := []string{"localhost:2181"}

	eventBatchSize := 100
	go produceEvents(kafkaTopic, eventBatchSize)

	consumer, consumerErr := JoinConsumerGroup(consumerGroupName, kafkaTopic, zookeeper, nil)
	if consumerErr != nil {
		t.Fatal(consumerErr)
	}
	defer consumer.Close()

	eventCount := 0
	offsets := make(map[int32]int64)

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
			if offsets[event.Partition] != 0 && offsets[event.Partition] != event.Offset-1 {
				t.Fatalf("Unexpected offset on partition %d: %d. Expected %d\n", event.Partition, event.Offset, offsets[event.Partition]+1)
			} else {
				log.Printf("Consumed message %d", eventCount)
			}

			offsets[event.Partition] = event.Offset
		}
	}
	log.Println("Bye")
}
