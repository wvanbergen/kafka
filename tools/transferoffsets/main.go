package main

import (
	"flag"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

var (
	zookeeper = flag.String("zookeeper", os.Getenv("ZOOKEEPER"), "The zookeeper connection string")
	groupName = flag.String("group", "", "The consumer group to transfer offsets for")
)

func main() {
	flag.Parse()

	if *zookeeper == "" {
		log.Fatal("The -zookeeper command line argument is required")
	}

	if *groupName == "" {
		log.Fatal("The -group command line argument is required")
	}

	var (
		config         = kazoo.NewConfig()
		zookeeperNodes []string
	)

	zookeeperNodes, config.Chroot = kazoo.ParseConnectionString(*zookeeper)
	kz, err := kazoo.NewKazoo(zookeeperNodes, config)
	if err != nil {
		log.Fatal("Failed to connect to the zookeeper cluster:", err)
	}
	defer kz.Close()

	brokerList, err := kz.BrokerList()
	if err != nil {
		log.Fatal("Failed to retrieve Kafka broker list from zookeeper:", err)
	}

	group := kz.Consumergroup(*groupName)
	if exists, err := group.Exists(); err != nil {
		log.Fatal(err)
	} else if !exists {
		log.Fatalf("The consumergroup %s is not registered in Zookeeper", groupName)
	}

	offsets, err := group.FetchAllOffsets()
	if err != nil {
		log.Fatal("Failed to retrieve offsets from zookeeper:", err)
	}

	client, err := sarama.NewClient(brokerList, nil)
	if err != nil {
		log.Fatal("Failed to connect to Kafka cluster:", err)
	}
	defer client.Close()

	coordinator, err := client.Coordinator(group.Name)
	if err != nil {
		log.Fatal("Failed to obtain coordinator for consumer group:", err)
	}

	request := &sarama.OffsetCommitRequest{
		Version:       1,
		ConsumerGroup: group.Name,
	}

	for topic, partitionOffsets := range offsets {
		for partition, nextOffset := range partitionOffsets {
			// In Zookeeper, we store the next offset to process.
			// In Kafka, we store the last offset that was processed.
			// So we have to fix an off by one error.
			lastOffset := offset - 1
			log.Printf("Last processed offset for %s/%d: %d", topic, partition, lastOffset)
			request.AddBlock(topic, partition, lastOffset, 0, "")
		}
	}

	response, err := coordinator.CommitOffset(request)
	if err != nil {
		log.Fatal("Failed to commit offsets to Kafka:", err)
	}

	var errorsFound = false
	for topic, partitionErrors := range response.Errors {
		for partition, err := range partitionErrors {
			if err != sarama.ErrNoError {
				errorsFound = true
				log.Printf("WARNING: offset for %s/%d was not committed: %s", topic, partition, err)
			}
		}
	}

	if !errorsFound {
		log.Print("Offsets successfully committed to Kafka!")
	}
}
