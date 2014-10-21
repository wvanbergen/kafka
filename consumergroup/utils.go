package consumergroup

import (
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

// Divides a set of partitions between a set of consumers.
func dividePartitionsBetweenConsumers(consumers []string, partitions partitionLeaderSlice) map[string]partitionLeaderSlice {
	result := make(map[string]partitionLeaderSlice)

	plen := len(partitions)
	clen := len(consumers)

	sort.Sort(partitions)
	sort.Strings(consumers)

	n := int(math.Ceil(float64(plen) / float64(clen)))
	for i, consumer := range consumers {
		first := i * n
		if first > plen {
			first = plen
		}

		last := (i + 1) * n
		if last > plen {
			last = plen
		}

		result[consumer] = partitions[first:last]
	}

	return result
}

// Partition information
type partitionLeader struct {
	id     int32
	leader int
}

// A sortable slice of Partition structs
type partitionLeaderSlice []partitionLeader

func (s partitionLeaderSlice) Len() int {
	return len(s)
}

func (s partitionLeaderSlice) Less(i, j int) bool {
	return s[i].leader < s[j].leader || (s[i].leader == s[j].leader && s[i].id < s[j].id)
}

func (s partitionLeaderSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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
