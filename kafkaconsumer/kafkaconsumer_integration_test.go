package kafkaconsumer

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

const (
	TopicWithSinglePartition    = "test.1"
	TopicWithMultiplePartitions = "test.4"
)

var (
	// By default, assume we're using Sarama's vagrant cluster when running tests
	zookeeper = kazoo.BuildConnectionString([]string{"192.168.100.67:2181", "192.168.100.67:2182", "192.168.100.67:2183", "192.168.100.67:2184", "192.168.100.67:2185"})
)

func init() {
	if os.Getenv("ZOOKEEPER_PEERS") != "" {
		zookeeper = os.Getenv("ZOOKEEPER_PEERS")
	}

	if os.Getenv("DEBUG") != "" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	fmt.Printf("Using Zookeeper cluster at %s\n", zookeeper)
}

////////////////////////////////////////////////////////////////////
// Examples
////////////////////////////////////////////////////////////////////

// This example sets up a consumer instance that consumes two topics,
// processes and commits the messages that are consumed, and properly
// shuts down the consumer when the process is interrupted.
func ExampleConsumer() {
	consumer, err := Join(
		"ExampleConsumerGroup",                       // name of the consumer group
		TopicSubscription("access_log", "audit_log"), // topics to subscribe to
		"zk1:2181,zk2:2181,zk3:2181/chroot",          // zookeeper connection string
		nil) // Set this to a *Config instance to override defaults

	if err != nil {
		log.Fatalln(err)
	}

	// Trap the interrupt signal to cleanly shut down the consumer
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		consumer.Interrupt()
	}()

	eventCount := 0
	for message := range consumer.Messages() {
		// Process message
		log.Println(string(message.Value))
		eventCount += 1

		// Acknowledge that the message has been processed
		consumer.Ack(message)
	}

	log.Printf("Processed %d events.", eventCount)
}

////////////////////////////////////////////////////////////////////
// Integration tests
////////////////////////////////////////////////////////////////////

func TestFunctionalSingleConsumerSingleTopic(t *testing.T) {
	offsetTotal := setupOffsets(t, "kafkaconsumer.TestSingleConsumerSingleTopic", []string{"test.4"})
	produceMessages(t, "test.4", 100)

	consumer, err := Join("kafkaconsumer.TestSingleConsumerSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}
	defer consumer.Close()

	assertMessages(t, consumer, 100)
	assertOffsets(t, []string{"test.4"}, offsetTotal, 100)
}

func TestFunctionalSingleConsumerMultipleTopics(t *testing.T) {
	offsetTotal := setupOffsets(t, "kafkaconsumer.TestSingleConsumerMultipleTopics", []string{"test.4", "test.1"})
	produceMessages(t, "test.4", 100)
	produceMessages(t, "test.1", 200)

	consumer, err := Join("kafkaconsumer.TestSingleConsumerMultipleTopics", TopicSubscription("test.4", "test.1"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}
	defer consumer.Close()

	assertMessages(t, consumer, 300)
	assertOffsets(t, []string{"test.4", "test.1"}, offsetTotal, 300)
}

// For this test, we produce 100 messages, and then consume the first 50 messages with
// the first consumer, and the last 50 messages with a second consumer
func TestFunctionalSerialConsumersSingleTopic(t *testing.T) {
	offsetTotal := setupOffsets(t, "kafkaconsumer.TestSerialConsumersSingleTopic", []string{"test.4"})
	produceMessages(t, "test.4", 100)

	consumer1, err := Join("kafkaconsumer.TestSerialConsumersSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	assertMessages(t, consumer1, 50)
	consumer1.Close()

	consumer2, err := Join("kafkaconsumer.TestSerialConsumersSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	assertMessages(t, consumer2, 50)
	consumer2.Close()

	assertOffsets(t, []string{"test.4"}, offsetTotal, 100)
}

func TestFunctionalParallelConsumersSingleTopic(t *testing.T) {
	offsetTotal := setupOffsets(t, "kafkaconsumer.TestFunctionalParallelConsumersSingleTopic", []string{"test.4"})
	produceMessages(t, "test.4", 100)

	consumer1, err := Join("kafkaconsumer.TestFunctionalParallelConsumersSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	consumer2, err := Join("kafkaconsumer.TestFunctionalParallelConsumersSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	// We start consuming messages from both consumers in parallel.
	// Both should acknowledge 25 messages.

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		assertMessages(t, consumer1, 25)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		assertMessages(t, consumer2, 25)
	}()

	wg.Wait()

	// Now, we close one consumer, which means that the second consumer will consume all other messages.
	consumer1.Close()

	assertMessages(t, consumer2, 50)

	consumer2.Close()
	assertOffsets(t, []string{"test.4"}, offsetTotal, 100)
}

////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////

func assertMessages(t *testing.T, consumer Consumer, total int) {
	timeout := time.After(10 * time.Second)
	count := 0
	for {
		select {
		case <-timeout:
			t.Errorf("TIMEOUT: only consumed %d/%d messages", count, total)
			return

		case msg := <-consumer.Messages():
			count++
			consumer.Ack(msg)

			if count == total {
				t.Logf("Consumed all %d messages.", total)
				return
			}
		}
	}
}

func saramaClient(t *testing.T) sarama.Client {
	zkNodes, _ := kazoo.ParseConnectionString(zookeeper)
	kz, err := kazoo.NewKazoo(zkNodes, nil)
	if err != nil {
		t.Fatal("Failed to connect to Zookeeper:", err)
	}
	defer kz.Close()

	brokers, err := kz.BrokerList()
	if err != nil {
		t.Fatal("Failed to retrieve broker list from Zookeeper:", err)
	}

	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		t.Fatal("Failed to connect to Kafka:", err)
	}

	return client
}

func produceMessages(t *testing.T, topic string, count int) {
	client := saramaClient(t)
	defer client.Close()

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		t.Fatal("Failed to open Kafka producer:", err)
	}
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			t.Errorf("Failed to produce message:", err)
		}
	}()

	for i := 0; i < count; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("%d", count)),
		}
	}
}

func assertOffsets(t *testing.T, topics []string, offsetTotal int64, count int) {
	client := saramaClient(t)
	defer client.Close()

	var newOffsetTotal int64
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			t.Fatal("Failed to retrieve list of partitions:", err)
		}

		for _, partition := range partitions {

			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				t.Fatal("Failed to get latest offset for partition:", err)
			}

			newOffsetTotal += offset
		}
	}

	if newOffsetTotal-offsetTotal != int64(count) {
		t.Errorf("Expected the offsets to have increased by %d, but increment was %d", count, newOffsetTotal-offsetTotal)
	} else {
		t.Logf("The offsets stored in Kafka have increased by %d", count)
	}
}

func setupOffsets(t *testing.T, group string, topics []string) int64 {
	client := saramaClient(t)
	defer client.Close()

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		t.Fatal("Failed to open Kafka offset manager:", err)
	}

	var offsetTotal int64
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			t.Fatal("Failed to retrieve list of partitions:", err)
		}

		for _, partition := range partitions {
			pom, err := offsetManager.ManagePartition(topic, partition)
			if err != nil {
				t.Fatal("Failed to start offset manager for partition:", err)
			}
			defer pom.Close()

			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				t.Fatal("Failed to get latest offset for partition:", err)
			}

			t.Logf("Setting initial offset for %s/%d: %d.", topic, partition, offset)
			pom.SetOffset(offset-1, "Set by integration test")
			offsetTotal += offset
		}
	}
	return offsetTotal
}
