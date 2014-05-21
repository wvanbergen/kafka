package consumergroup

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

var (
	clientConfig   = sarama.ClientConfig{MetadataRetries: 5, WaitForElection: 250 * time.Millisecond}
	consumerConfig = sarama.ConsumerConfig{MaxWaitTime: 500000, DefaultFetchSize: 256 * 1024, MinFetchSize: 1024, OffsetMethod: sarama.OffsetMethodOldest}
)

// COMMON TYPES

// Partition information
type Partition struct {
	Id   int32
	Addr string // Leader address
}

// A sortable slice of Partition structs
type PartitionSlice []Partition

func (s PartitionSlice) Len() int { return len(s) }
func (s PartitionSlice) Less(i, j int) bool {
	if s[i].Addr < s[j].Addr {
		return true
	}
	return s[i].Id < s[j].Id
}
func (s PartitionSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// A subscribable notification
type Notification struct {
	Type uint8
	Src  *ConsumerGroup
	Err  error
}

func generateUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func generateConsumerID() (consumerID string, err error) {
	var uuid, hostname string

	uuid, err = generateUUID()
	if err != nil {
		return
	}

	hostname, err = os.Hostname()
	if err != nil {
		return
	}

	consumerID = fmt.Sprintf("%s:%s", hostname, uuid)
	return
}
