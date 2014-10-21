package consumergroup

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// ZK wraps a zookeeper connection
type ZK struct {
	*zk.Conn
	chroot string
}

// NewZK creates a new connection instance
func NewZK(servers []string, chroot string, recvTimeout time.Duration) (*ZK, error) {
	conn, _, err := zk.Connect(servers, recvTimeout)
	if err != nil {
		return nil, err
	}
	return &ZK{conn, chroot}, nil
}

/*******************************************************************
 * HIGH LEVEL API
 *******************************************************************/

func (z *ZK) Brokers() (map[int]string, error) {
	root := fmt.Sprintf("%s/brokers/ids", z.chroot)
	children, _, err := z.Children(root)
	if err != nil {
		return nil, err
	}

	type brokerEntry struct {
		Host string `json:host`
		Port int    `json:port`
	}

	result := make(map[int]string)
	for _, child := range children {
		brokerID, err := strconv.ParseInt(child, 10, 32)
		if err != nil {
			return nil, err
		}

		value, _, err := z.Get(path.Join(root, child))
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

// Consumers returns all active consumers within a group
func (z *ZK) Consumers(group string) ([]string, <-chan zk.Event, error) {
	root := fmt.Sprintf("%s/consumers/%s/ids", z.chroot, group)
	err := z.MkdirAll(root)
	if err != nil {
		return nil, nil, err
	}

	strs, _, ch, err := z.ChildrenW(root)
	if err != nil {
		return nil, nil, err
	}
	return strs, ch, nil
}

func (z *ZK) Partitions(topic string) (partitionLeaderSlice, error) {
	root := fmt.Sprintf("%s/brokers/topics/%s/partitions", z.chroot, topic)
	children, _, err := z.Children(root)
	if err != nil {
		return nil, err
	}

	type partitionEntry struct {
		Leader int `json:leader`
	}

	result := make(partitionLeaderSlice, 0, len(children))
	for _, child := range children {
		partitionNumber, err := strconv.ParseInt(child, 10, 32)
		if err != nil {
			return nil, err
		}

		value, _, err := z.Get(path.Join(root, child, "state"))
		if err != nil {
			return nil, err
		}

		var partitionNode partitionEntry
		if err := json.Unmarshal(value, &partitionNode); err != nil {
			return nil, err
		}

		result = append(result, partitionLeader{id: int32(partitionNumber), leader: partitionNode.Leader})
	}

	return result, nil
}

// Claim claims a topic/partition ownership for a consumer ID within a group
func (z *ZK) Claim(group, topic string, partition int32, id string) (err error) {
	root := fmt.Sprintf("%s/consumers/%s/owners/%s", z.chroot, group, topic)
	if err = z.MkdirAll(root); err != nil {
		return err
	}

	node := fmt.Sprintf("%s/%d", root, partition)
	tries := 0
	for {
		if err = z.Create(node, []byte(id), true); err == nil {
			break
		} else if tries++; err != zk.ErrNodeExists || tries > 100 {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

// Release releases a claim
func (z *ZK) Release(group, topic string, partition int32, id string) error {
	node := fmt.Sprintf("%s/consumers/%s/owners/%s/%d", z.chroot, group, topic, partition)
	val, _, err := z.Get(node)

	// Already deleted
	if err == zk.ErrNoNode {
		return nil
	}

	// Locked by someone else?
	if string(val) != id {
		return zk.ErrNotLocked
	}

	return z.DeleteAll(node)
}

// Commit commits an offset to a group/topic/partition
func (z *ZK) Commit(group, topic string, partition int32, offset int64) (err error) {
	root := fmt.Sprintf("%s/consumers/%s/offsets/%s", z.chroot, group, topic)
	if err = z.MkdirAll(root); err != nil {
		return err
	}

	node := fmt.Sprintf("%s/%d", root, partition)
	data := []byte(fmt.Sprintf("%d", offset))
	_, stat, err := z.Get(node)

	// Try to create new node
	if err == zk.ErrNoNode {
		return z.Create(node, data, false)
	} else if err != nil {
		return err
	}

	_, err = z.Set(node, data, stat.Version)
	return
}

// Offset retrieves an offset to a group/topic/partition
func (z *ZK) Offset(group, topic string, partition int32) (int64, error) {
	node := fmt.Sprintf("%s/consumers/%s/offsets/%s/%d", z.chroot, group, topic, partition)
	val, _, err := z.Get(node)
	if err == zk.ErrNoNode {
		return 0, nil
	} else if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(val), 10, 64)
}

// RegisterGroup creates/updates a group directory
func (z *ZK) RegisterGroup(group string) error {
	return z.MkdirAll(fmt.Sprintf("%s/consumers/%s/ids", z.chroot, group))
}

// CreateConsumer registers a new consumer within a group
func (z *ZK) RegisterConsumer(group, id string, topics []string) error {
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

	return z.Create(fmt.Sprintf("%s/consumers/%s/ids/%s", z.chroot, group, id), data, true)
}

func (z *ZK) DeregisterConsumer(group, id string) error {
	return z.Delete(fmt.Sprintf("%s/consumers/%s/ids/%s", z.chroot, group, id), 0)
}

/*******************************************************************
 * LOW LEVEL API
 *******************************************************************/

// Exists checks existence of a node
func (z *ZK) Exists(node string) (ok bool, err error) {
	ok, _, err = z.Conn.Exists(node)
	return
}

// DeleteAll deletes a node recursively
func (z *ZK) DeleteAll(node string) (err error) {
	children, stat, err := z.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = z.DeleteAll(path.Join(node, child)); err != nil {
			return
		}
	}

	return z.Delete(node, stat.Version)
}

// MkdirAll creates a directory recursively
func (z *ZK) MkdirAll(node string) (err error) {
	parent := path.Dir(node)
	if parent != "/" {
		if err = z.MkdirAll(parent); err != nil {
			return
		}
	}

	_, err = z.Conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return
}

// Create stores a new value at node. Fails if already set.
func (z *ZK) Create(node string, value []byte, ephemeral bool) (err error) {
	if err = z.MkdirAll(path.Dir(node)); err != nil {
		return
	}

	flags := int32(0)
	if ephemeral {
		flags = zk.FlagEphemeral
	}
	_, err = z.Conn.Create(node, value, flags, zk.WorldACL(zk.PermAll))
	return
}
