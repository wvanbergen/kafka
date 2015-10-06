package kafkaconsumer

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
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
	zookeeper    = kazoo.BuildConnectionString([]string{"192.168.100.67:2181", "192.168.100.67:2182", "192.168.100.67:2183", "192.168.100.67:2184", "192.168.100.67:2185"})
	kafkaBrokers []string
)

func init() {
	if os.Getenv("ZOOKEEPER_PEERS") != "" {
		zookeeper = os.Getenv("ZOOKEEPER_PEERS")
	}

	if os.Getenv("DEBUG") != "" {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	fmt.Printf("Using Zookeeper cluster at %s\n", zookeeper)

	zkNodes, _ := kazoo.ParseConnectionString(zookeeper)
	kz, err := kazoo.NewKazoo(zkNodes, nil)
	if err != nil {
		log.Fatal("Failed to connect to Zookeeper:", err)
	}
	defer kz.Close()

	brokers, err := kz.BrokerList()
	if err != nil {
		log.Fatal("Failed to retrieve broker list from Zookeeper:", err)
	}

	kafkaBrokers = brokers
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
	ts := newTestState(t, "kafkaconsumer.TestSingleConsumerSingleTopic")
	ts.prepareConsumer([]string{"test.4"})
	ts.produceMessages("test.4", 100)

	consumer, err := Join("kafkaconsumer.TestSingleConsumerSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	ts.assertMessages(consumer, 100)

	safeClose(t, consumer)
	ts.assertDrained(consumer)

	ts.assertOffsets()
}

func TestFunctionalSingleConsumerMultipleTopics(t *testing.T) {
	ts := newTestState(t, "kafkaconsumer.TestSingleConsumerMultipleTopics")
	ts.prepareConsumer([]string{"test.4", "test.1"})

	ts.produceMessages("test.4", 100)
	ts.produceMessages("test.1", 200)

	consumer, err := Join("kafkaconsumer.TestSingleConsumerMultipleTopics", TopicSubscription("test.4", "test.1"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	ts.assertMessages(consumer, 300)

	safeClose(t, consumer)
	ts.assertDrained(consumer)

	ts.assertOffsets()
}

// For this test, we produce 100 messages, and then consume the first 50 messages with
// the first consumer, and the last 50 messages with a second consumer
func TestFunctionalSerialConsumersSingleTopic(t *testing.T) {
	ts := newTestState(t, "kafkaconsumer.TestSerialConsumersSingleTopic")
	ts.prepareConsumer([]string{"test.4"})
	ts.produceMessages("test.4", 100)

	consumer1, err := Join("kafkaconsumer.TestSerialConsumersSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	ts.assertMessages(consumer1, 50)

	safeClose(t, consumer1)
	// Consumer 1 may not be fully drained, but the messages that are in the pipeline won't
	// be committed, and will eventually be consumed by consumer2 instead

	consumer2, err := Join("kafkaconsumer.TestSerialConsumersSingleTopic", TopicSubscription("test.4"), zookeeper, nil)
	if err != nil {
		t.Fatal(t)
	}

	ts.assertMessages(consumer2, 50)

	safeClose(t, consumer2)
	// Consumer 2 should be fully drained though
	ts.assertDrained(consumer2)

	ts.assertOffsets()
}

func TestFunctionalParallelConsumers(t *testing.T) {
	ts := newTestState(t, "kafkaconsumer.TestFunctionalParallelConsumers")
	ts.prepareConsumer([]string{"test.64"})
	ts.produceMessages("test.64", 1000)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			consumer, err := Join("kafkaconsumer.TestFunctionalParallelConsumers", TopicSubscription("test.64"), zookeeper, nil)
			if err != nil {
				t.Fatal(t)
			}

			go func() {
				<-ts.done
				consumer.Interrupt()
			}()

			ts.assertAllMessages(consumer)
		}()
	}

	wg.Wait()
	ts.assertOffsets()
}

func TestFunctionalParallelConsumersWithInterruption(t *testing.T) {
	ts := newTestState(t, "kafkaconsumer.TestFunctionalParallelConsumersWithInterruption")
	ts.prepareConsumer([]string{"test.4"})
	ts.produceMessages("test.4", 100)

	config := NewConfig()
	config.MaxProcessingTime = 500 * time.Millisecond

	consumer1, err := Join("kafkaconsumer.TestFunctionalParallelConsumersWithInterruption", TopicSubscription("test.4"), zookeeper, config)
	if err != nil {
		t.Fatal(t)
	}

	consumer2, err := Join("kafkaconsumer.TestFunctionalParallelConsumersWithInterruption", TopicSubscription("test.4"), zookeeper, config)
	if err != nil {
		t.Fatal(t)
	}

	// We start consuming messages from both consumers in parallel.
	// Both should acknowledge 25 messages.

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ts.assertMessages(consumer1, 25)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ts.assertMessages(consumer2, 25)
	}()

	wg.Wait()

	// Now, we close one consumer, which means that the second consumer will consume all other messages.
	safeClose(t, consumer1)

	ts.assertMessages(consumer2, 50)

	// Now, close the second consumer
	safeClose(t, consumer2)
	ts.assertDrained(consumer2)

	ts.assertOffsets()
}

////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////

func safeClose(t *testing.T, c io.Closer) {
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func newTestState(t *testing.T, group string) *testState {
	return &testState{t: t, group: group, done: make(chan struct{})}
}

type testState struct {
	t           *testing.T
	group       string
	c           sarama.Client
	coord       *sarama.Broker
	offsetTotal int64
	topics      []string
	produced    int64
	consumed    int64
	done        chan struct{}
}

func (ts *testState) close() {
	if ts.c != nil {
		safeClose(ts.t, ts.c)
	}
}

func (ts *testState) client() sarama.Client {
	if ts.c == nil {
		config := sarama.NewConfig()
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

		client, err := sarama.NewClient(kafkaBrokers, config)
		if err != nil {
			ts.t.Fatal("Failed to connect to Kafka:", err)
		}

		ts.c = client
	}

	return ts.c
}

func (ts *testState) coordinator() *sarama.Broker {
	if ts.coord == nil {
		coordinator, err := ts.client().Coordinator(ts.group)
		if err != nil {
			ts.t.Fatal(err)
		}
		ts.coord = coordinator
	}

	return ts.coord
}

func (ts *testState) prepareConsumer(topics []string) {
	client := ts.client()
	coordinator := ts.coordinator()

	request := &sarama.OffsetCommitRequest{
		Version:       1,
		ConsumerGroup: ts.group,
	}

	ts.topics = topics
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			ts.t.Fatal("Failed to retrieve list of partitions:", err)
		}

		for _, partition := range partitions {
			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				ts.t.Fatal("Failed to get latest offset for partition:", err)
			}
			ts.offsetTotal += offset - 1

			request.AddBlock(topic, partition, offset-1, -1, "")
			ts.t.Logf("Setting initial offset for %s/%d: %d.", topic, partition, offset)
		}
	}

	response, err := coordinator.CommitOffset(request)
	if err != nil {
		ts.t.Fatal("Failed to commit offsets for consumergroup:", err)
	}

	for topic, errors := range response.Errors {
		for partition, err := range errors {
			if err != sarama.ErrNoError {
				ts.t.Fatalf("Failed to commit offsets for %s/%d: %s", topic, partition, err)
			}
		}
	}
}

func (ts *testState) produceMessages(topic string, count int64) {
	producer, err := sarama.NewAsyncProducerFromClient(ts.client())
	if err != nil {
		ts.t.Fatal("Failed to open Kafka producer:", err)
	}
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			ts.t.Error("Failed to produce message:", err)
		}
	}()

	for i := int64(0); i < count; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("%d", count)),
		}
	}

	atomic.AddInt64(&ts.produced, count)
	ts.t.Logf("Produced %d messages to %s", count, topic)
}

func (ts *testState) assertAllMessages(consumer Consumer) {
	var count int64
	for msg := range consumer.Messages() {
		count++
		atomic.AddInt64(&ts.consumed, 1)
		consumer.Ack(msg)

		if atomic.LoadInt64(&ts.produced) == atomic.LoadInt64(&ts.consumed) {
			close(ts.done)
		}
	}

	ts.t.Logf("Consumed %d out of %d messages.", count, atomic.LoadInt64(&ts.produced))
}

func (ts *testState) assertMessages(consumer Consumer, total int) {
	timeout := time.After(10 * time.Second)
	count := 0
	for {
		select {
		case <-timeout:
			ts.t.Errorf("TIMEOUT: only consumed %d/%d messages", count, total)
			return

		case msg := <-consumer.Messages():
			count++
			atomic.AddInt64(&ts.consumed, 1)
			consumer.Ack(msg)

			if count == total {
				ts.t.Logf("Consumed %d out of %d messages.", total, atomic.LoadInt64(&ts.produced))
				return
			}
		}
	}
}

func (ts *testState) assertDrained(consumer Consumer) {
	if _, ok := <-consumer.Messages(); ok {
		ts.t.Error("Expected messages channel of consumer to be drained")
	}
}

func (ts *testState) assertOffsets() {
	defer ts.close()

	request := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: ts.group,
	}

	for _, topic := range ts.topics {
		partitions, err := ts.client().Partitions(topic)
		if err != nil {
			ts.t.Fatal("Failed to retrieve list of partitions:", err)
		}

		for _, partition := range partitions {
			request.AddPartition(topic, partition)
		}
	}

	response, err := ts.coordinator().FetchOffset(request)
	if err != nil {
		ts.t.Fatal("Failed to fetch committed offsets", err)
	}

	var newOffsetTotal int64
	for topic, partitions := range response.Blocks {
		for partition, block := range partitions {
			if block.Err != sarama.ErrNoError {
				ts.t.Fatalf("%s/%d: %s", topic, partition, block.Err)
			}

			ts.t.Logf("Committed offset for %s/%d: %d", topic, partition, block.Offset)
			newOffsetTotal += block.Offset
		}
	}

	produced := atomic.LoadInt64(&ts.produced)
	if newOffsetTotal-ts.offsetTotal != produced {
		ts.t.Errorf("Expected the offsets to have increased by %d, but increment was %d", produced, newOffsetTotal-ts.offsetTotal)
	} else {
		ts.t.Logf("The offsets stored in Kafka have increased by %d", produced)
	}
}
