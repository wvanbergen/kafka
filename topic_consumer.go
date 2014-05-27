package main

import (
	"flag"
	"fmt"
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
	kafkaTopic    string
	consumerGroup string
	zookeeper     []string
)

func init() {
	kafkaTopic = *flag.String("topic", DefaultKafkaTopic, "The topic to consume")
	consumerGroup = *flag.String("group", DefaultConsumerGroup, "The name of the consumer group, used for coordination and load balancing")
	zookeeperCSV := flag.String("zookeeper", "", "A comma-separated Zookeeper connection string (e.g. `zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181`)")

	flag.Parse()

	if *zookeeperCSV == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	zookeeper = strings.Split(*zookeeperCSV, ",")

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

func main() {
	log.Printf("Joining consumer group for %s...", kafkaTopic)
	consumer, consumerErr := consumergroup.JoinConsumerGroup(consumerGroup, kafkaTopic, zookeeper, nil)
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
	offsets := make(map[int32]int64)

	err := consumer.Process(func(event *sarama.ConsumerEvent) error {
		eventCount += 1
		if offsets[event.Partition] != 0 && offsets[event.Partition] != event.Offset-1 {

			return fmt.Errorf("Unexpected offset on partition %d: %d. Expected %d", event.Partition, event.Offset, offsets[event.Partition]+1)
		}

		offsets[event.Partition] = event.Offset
		return nil
	})

	if err != nil {
		log.Println("ERROR", err)
	}

	log.Printf("Processed %d events.", eventCount)
}
