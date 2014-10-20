package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	DefaultKafkaTopic    = "nginx.multitrack"
	DefaultConsumerGroup = "topic_consumer.go"
)

var (
	consumerGroup string
	kafkaTopics   []string
	zookeeper     []string
)

func init() {
	consumerGroup = *flag.String("group", DefaultConsumerGroup, "The name of the consumer group, used for coordination and load balancing")
	kafkaTopicsCSV := flag.String("topics", DefaultKafkaTopic, "The comma-separated list of topics to consume")
	zookeeperCSV := flag.String("zookeeper", "", "A comma-separated Zookeeper connection string (e.g. `zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181`)")

	flag.Parse()

	if *zookeeperCSV == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	zookeeper = strings.Split(*zookeeperCSV, ",")
	kafkaTopics = strings.Split(*kafkaTopicsCSV, ",")

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

func main() {
	log.Printf("Joining consumer group for %s...", strings.Join(kafkaTopics, ", "))
	consumer, consumerErr := consumergroup.JoinConsumerGroup(consumerGroup, kafkaTopics, zookeeper, nil)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			sarama.Logger.Println("Error closing the consumer", err)
		}
	}()

	eventCount := 0
	offsets := make(map[int32]int64)

	stream := consumer.Events()
	for {
		event, ok := <-stream
		if !ok {
			break
		}

		eventCount += 1
		if offsets[event.Partition] != 0 && offsets[event.Partition] != event.Offset-1 {
			log.Printf("Unexpected offset on partition %d: %d. Expected %d\n", event.Partition, event.Offset, offsets[event.Partition]+1)
		}

		offsets[event.Partition] = event.Offset
	}

	log.Printf("Processed %d events.", eventCount)
}
