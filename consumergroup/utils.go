package consumergroup

import (
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"github.com/wvanbergen/kafka/kazoo"
)

// Divides a set of partitions between a set of consumers.
func dividePartitionsBetweenConsumers(consumers []string, partitions []kazoo.Partition) map[string]PartitionLeaders {
	result := make(map[string]PartitionLeaders)

	plen := len(partitions)
	clen := len(consumers)

	partitionLeaders := PartitionLeaders(partitions)
	sort.Sort(partitionLeaders)
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

// A sortable slice of PartitionLeader structs
type PartitionLeaders []kazoo.Partition

func (pls PartitionLeaders) Len() int {
	return len(pls)
}

func (pls PartitionLeaders) Less(i, j int) bool {
	return pls[i].Leader < pls[j].Leader || (pls[i].Leader == pls[j].Leader && pls[i].ID < pls[j].ID)
}

func (s PartitionLeaders) Swap(i, j int) {
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
