package kazoo

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	FailedToClaimPartition = errors.New("Failed to claim partition for this consumer instance. Do you have a rogue consumer running?")
)

// Kazoo interacts with the Kafka metadata in Zookeeper
type Kazoo struct {
	conn *zk.Conn
	conf *Config
}

type Config struct {
	Chroot  string
	Timeout time.Duration
}

func NewConfig() *Config {
	return &Config{Timeout: 1 * time.Second}
}

// NewKazoo creates a new connection instance
func NewKazoo(servers []string, conf *Config) (*Kazoo, error) {
	if conf == nil {
		conf = NewConfig()
	}

	conn, _, err := zk.Connect(servers, conf.Timeout)
	if err != nil {
		return nil, err
	}
	return &Kazoo{conn, conf}, nil
}

func (kz *Kazoo) Brokers() (map[int32]string, error) {
	root := fmt.Sprintf("%s/brokers/ids", kz.conf.Chroot)
	children, _, err := kz.conn.Children(root)
	if err != nil {
		return nil, err
	}

	type brokerEntry struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	result := make(map[int32]string)
	for _, child := range children {
		brokerID, err := strconv.ParseInt(child, 10, 32)
		if err != nil {
			return nil, err
		}

		value, _, err := kz.conn.Get(path.Join(root, child))
		if err != nil {
			return nil, err
		}

		var brokerNode brokerEntry
		if err := json.Unmarshal(value, &brokerNode); err != nil {
			return nil, err
		}

		result[int32(brokerID)] = fmt.Sprintf("%s:%d", brokerNode.Host, brokerNode.Port)
	}

	return result, nil
}

// Controller returns what broker is currently acting as controller of the Kafka cluster
func (kz *Kazoo) Controller() (int32, error) {
	type controllerEntry struct {
		BrokerID int32 `json:"brokerid"`
	}

	node := fmt.Sprintf("%s/controller", kz.conf.Chroot)
	data, _, err := kz.conn.Get(node)
	if err != nil {
		return -1, err
	}

	var controllerNode controllerEntry
	if err := json.Unmarshal(data, &controllerNode); err != nil {
		return -1, err
	}

	return controllerNode.BrokerID, nil
}

func (kz *Kazoo) Close() error {
	kz.conn.Close()
	return nil
}

////////////////////////////////////////////////////////////////////////
// Util methods
////////////////////////////////////////////////////////////////////////

// Exists checks existence of a node
func (kz *Kazoo) exists(node string) (ok bool, err error) {
	ok, _, err = kz.conn.Exists(node)
	return
}

// DeleteAll deletes a node recursively
func (kz *Kazoo) deleteRecursive(node string) (err error) {
	children, stat, err := kz.conn.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = kz.deleteRecursive(path.Join(node, child)); err != nil {
			return
		}
	}

	return kz.conn.Delete(node, stat.Version)
}

// MkdirAll creates a directory recursively
func (kz *Kazoo) mkdirRecursive(node string) (err error) {
	parent := path.Dir(node)
	if parent != "/" {
		if err = kz.mkdirRecursive(parent); err != nil {
			return
		}
	}

	_, err = kz.conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return
}

// Create stores a new value at node. Fails if already set.
func (kz *Kazoo) create(node string, value []byte, ephemeral bool) (err error) {
	if err = kz.mkdirRecursive(path.Dir(node)); err != nil {
		return
	}

	flags := int32(0)
	if ephemeral {
		flags = zk.FlagEphemeral
	}
	_, err = kz.conn.Create(node, value, flags, zk.WorldACL(zk.PermAll))
	return
}
