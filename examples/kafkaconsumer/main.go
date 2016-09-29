package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/kafkaconsumer"
)

const (
	DefaultKafkaTopics   = "test_topic"
	DefaultConsumerGroup = "consumer_example.go"
)

var (
	consumerGroup  = flag.String("group", DefaultConsumerGroup, "The name of the consumer group, used for coordination and load balancing")
	kafkaTopicsCSV = flag.String("topics", DefaultKafkaTopics, "The comma-separated list of topics to consume")
	zookeeper      = flag.String("zookeeper", "", "A comma-separated Zookeeper connection string (e.g. `zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181`)")
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[sarama]        ", log.LstdFlags|log.Lshortfile)
	kafkaconsumer.Logger = log.New(os.Stdout, "[kafkaconsumer] ", log.LstdFlags|log.Lshortfile)
}

func main() {
	flag.Parse()

	if *zookeeper == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	config := kafkaconsumer.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest

	subscription := kafkaconsumer.TopicSubscription(strings.Split(*kafkaTopicsCSV, ",")...)
	consumer, err := kafkaconsumer.Join(*consumerGroup, subscription, *zookeeper, config)
	if err != nil {
		log.Fatalln(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		consumer.Interrupt()
	}()

	go func() {
		for err := range consumer.Errors() {
			log.Println(err)
		}
	}()

	var count int64
	for message := range consumer.Messages() {
		// Simulate processing that takes some time
		time.Sleep(10 * time.Millisecond)

		// Acknowledge that we have processed the message
		consumer.Ack(message)

		count++
	}

	log.Printf("Processed %d events.", count)
}
