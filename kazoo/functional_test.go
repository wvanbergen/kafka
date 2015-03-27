package kazoo

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

var (
	// By default, assume we're using Sarama's vagrant cluster when running tests
	zookeeperPeers []string = []string{"192.168.100.67:2181", "192.168.100.67:2182", "192.168.100.67:2183", "192.168.100.67:2184", "192.168.100.67:2185"}
)

func init() {
	if zookeeperPeersEnv := os.Getenv("ZOOKEEPER_PEERS"); zookeeperPeersEnv != "" {
		zookeeperPeers = strings.Split(zookeeperPeersEnv, ",")
	}

	fmt.Printf("Using Zookeeper cluster at %v\n", zookeeperPeers)
}

func TestTopics(t *testing.T) {
	kz, err := NewKazoo(zookeeperPeers, nil)
	if err != nil {
		t.Fatal(err)
	}

	topics, err := kz.Topics()
	if err != nil {
		t.Error(err)
	}
	t.Log(topics)

	assertSuccessfullClose(t, kz)
}

func assertSuccessfullClose(t *testing.T, kz *Kazoo) {
	if err := kz.Close(); err != nil {
		t.Error(err)
	}
}
