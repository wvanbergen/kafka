package kafkaconsumer

import (
	"fmt"
	"log"
	"os"
	// "os/signal"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	// "github.com/wvanbergen/kazoo-go"
)

const (
	TopicWithSinglePartition    = "test.1"
	TopicWithMultiplePartitions = "test.4"
)

var (
	// By default, assume we're using Sarama's vagrant cluster when running tests
	zookeeperPeers = []string{"192.168.100.67:2181", "192.168.100.67:2182", "192.168.100.67:2183", "192.168.100.67:2184", "192.168.100.67:2185"}
	kafkaPeers     = []string{"192.168.100.67:9091", "192.168.100.67:9092", "192.168.100.67:9093", "192.168.100.67:9094", "192.168.100.67:9095"}
)

func init() {
	if zookeeperPeersEnv := os.Getenv("ZOOKEEPER_PEERS"); zookeeperPeersEnv != "" {
		zookeeperPeers = strings.Split(zookeeperPeersEnv, ",")
	}
	if kafkaPeersEnv := os.Getenv("KAFKA_PEERS"); kafkaPeersEnv != "" {
		kafkaPeers = strings.Split(kafkaPeersEnv, ",")
	}

	if os.Getenv("DEBUG") != "" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	fmt.Printf("Using Zookeeper cluster at %v\n", zookeeperPeers)
	fmt.Printf("Using Kafka cluster at %v\n", kafkaPeers)
}

////////////////////////////////////////////////////////////////////
// Examples
////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////
// Integration tests
////////////////////////////////////////////////////////////////////

func TestConsumer(t *testing.T) {
	consumer, err := Join("consumergroup.TestConsumer", TopicSubscription("test.4"), strings.Join(zookeeperPeers, ","), nil)
	if err != nil {
		t.Fatal(t)
	}

	_ = consumer.Close()
}
