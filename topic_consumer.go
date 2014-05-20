package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/wvanbergen/kafkacluster/kafkaconsumer"
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
}

func main() {
	consumer, consumerErr := kafkaconsumer.NewKafkaConsumer(consumerGroup, zookeeper)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	log.Printf("Creating consumer for %s...", kafkaTopic)
	stream, streamErr := consumer.Stream(kafkaTopic)
	if streamErr != nil {
		log.Fatalln(streamErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		consumer.Close()
		os.Exit(130)
	}()

	offsets := make(map[int32]int64)

	for {
		event, ok := <-stream
		if !ok {
			log.Println("Consumer is done")
			return
		}

		if offsets[event.Partition] != 0 && offsets[event.Partition] != event.Offset-1 {
			log.Printf("Unexpected offset on partition %d: %d. Expected %d\n", event.Partition, event.Offset, offsets[event.Partition]+1)
		}

		offsets[event.Partition] = event.Offset
	}
}
