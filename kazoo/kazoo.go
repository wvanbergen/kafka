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

// Partition information
type Partition struct {
	Topic  string
	ID     int32
	Leader int32   `json:"leader"`
	ISR    []int32 `json:"isr"`
}

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

/*******************************************************************
 * HIGH LEVEL API
 *******************************************************************/

func (kz *Kazoo) Brokers() (map[int]string, error) {
	root := fmt.Sprintf("%s/brokers/ids", kz.conf.Chroot)
	children, _, err := kz.conn.Children(root)
	if err != nil {
		return nil, err
	}

	type brokerEntry struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	result := make(map[int]string)
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

		result[int(brokerID)] = fmt.Sprintf("%s:%d", brokerNode.Host, brokerNode.Port)
	}

	return result, nil
}

func (kz *Kazoo) Topics() ([]string, error) {
	root := fmt.Sprintf("%s/brokers/topics", kz.conf.Chroot)
	children, _, err := kz.conn.Children(root)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, child := range children {
		result = append(result, child)
	}

	return result, nil
}

func (kz *Kazoo) Partitions(topic string) ([]Partition, error) {
	root := fmt.Sprintf("%s/brokers/topics/%s/partitions", kz.conf.Chroot, topic)
	children, _, err := kz.conn.Children(root)
	if err != nil {
		return nil, err
	}

	result := make([]Partition, 0, len(children))
	for _, child := range children {
		partitionNumber, err := strconv.ParseInt(child, 10, 32)
		if err != nil {
			return nil, err
		}

		value, _, err := kz.conn.Get(path.Join(root, child, "state"))
		if err != nil {
			return nil, err
		}

		partition := Partition{Topic: topic, ID: int32(partitionNumber)}
		if err := json.Unmarshal(value, &partition); err != nil {
			return nil, err
		}

		result = append(result, partition)
	}

	return result, nil
}

// Consumers returns all active consumers within a group
func (kz *Kazoo) Consumers(group string) ([]string, <-chan zk.Event, error) {
	root := fmt.Sprintf("%s/consumers/%s/ids", kz.conf.Chroot, group)
	err := kz.mkdirRecursive(root)
	if err != nil {
		return nil, nil, err
	}

	strs, _, ch, err := kz.conn.ChildrenW(root)
	if err != nil {
		return nil, nil, err
	}
	return strs, ch, nil
}

// Claim claims a topic/partition ownership for a consumer ID within a group
func (kz *Kazoo) ClaimPartition(group, topic string, partition int32, id string) (err error) {
	root := fmt.Sprintf("%s/consumers/%s/owners/%s", kz.conf.Chroot, group, topic)
	if err = kz.mkdirRecursive(root); err != nil {
		return err
	}

	node := fmt.Sprintf("%s/%d", root, partition)
	tries := 0
	for {
		tries++
		if err = kz.create(node, []byte(id), true); err == nil {
			break
		} else if err != zk.ErrNodeExists {
			return err
		} else if tries > 100 {
			return FailedToClaimPartition
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

// Release releases a claim
func (kz *Kazoo) ReleasePartition(group, topic string, partition int32, id string) error {
	node := fmt.Sprintf("%s/consumers/%s/owners/%s/%d", kz.conf.Chroot, group, topic, partition)
	val, _, err := kz.conn.Get(node)

	// Already deleted
	if err == zk.ErrNoNode {
		return nil
	}

	// Locked by someone else?
	if string(val) != id {
		return zk.ErrNotLocked
	}

	return kz.deleteRecursive(node)
}

// CommitOffset commits an offset to a group/topic/partition
func (kz *Kazoo) CommitOffset(group, topic string, partition int32, offset int64) (err error) {
	root := fmt.Sprintf("%s/consumers/%s/offsets/%s", kz.conf.Chroot, group, topic)
	if err = kz.mkdirRecursive(root); err != nil {
		return err
	}

	node := fmt.Sprintf("%s/%d", root, partition)
	data := []byte(fmt.Sprintf("%d", offset))
	_, stat, err := kz.conn.Get(node)

	// Try to create new node
	if err == zk.ErrNoNode {
		return kz.create(node, data, false)
	} else if err != nil {
		return err
	}

	_, err = kz.conn.Set(node, data, stat.Version)
	return
}

// FetchOffset retrieves an offset to a group/topic/partition
func (kz *Kazoo) FetchOffset(group, topic string, partition int32) (int64, error) {
	node := fmt.Sprintf("%s/consumers/%s/offsets/%s/%d", kz.conf.Chroot, group, topic, partition)
	val, _, err := kz.conn.Get(node)
	if err == zk.ErrNoNode {
		return 0, nil
	} else if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(val), 10, 64)
}

// RegisterGroup creates/updates a group directory
func (kz *Kazoo) RegisterGroup(group string) error {
	return kz.mkdirRecursive(fmt.Sprintf("%s/consumers/%s/ids", kz.conf.Chroot, group))
}

// CreateConsumer registers a new consumer within a group
func (kz *Kazoo) RegisterConsumer(group, id string, topics []string) error {
	subscription := make(map[string]int)
	for _, topic := range topics {
		subscription[topic] = 1
	}

	data, err := json.Marshal(map[string]interface{}{
		"pattern":      "white_list",
		"subscription": subscription,
		"timestamp":    fmt.Sprintf("%d", time.Now().Unix()),
		"version":      1,
	})
	if err != nil {
		return err
	}

	return kz.create(fmt.Sprintf("%s/consumers/%s/ids/%s", kz.conf.Chroot, group, id), data, true)
}

func (kz *Kazoo) DeregisterConsumer(group, id string) error {
	return kz.conn.Delete(fmt.Sprintf("%s/consumers/%s/ids/%s", kz.conf.Chroot, group, id), 0)
}

func (kz *Kazoo) Close() error {
	kz.conn.Close()
	return nil
}

/*******************************************************************
 * LOW LEVEL API
 *******************************************************************/

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
