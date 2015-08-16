package kafkaconsumer

import (
	"sort"

	"github.com/wvanbergen/kazoo-go"
)

// retrievePartitionLeaders annotates a list of partitions with the leading broker.
// This list can be sorted and serve as input for distributePartitionsBetweenConsumers().
// By sorting them based on the leading broker, consumer instances will end up consuming
// partitions that are led by the same broker, which means less connections are necessary.
//
// Note: this implementation uses the preferred replica, not the actual leader. Normally,
// the preferred leader is also the actual leader, especially if auto.leader.rebalance.enable
// is set to true on the Kafka cluster. Using the actual leaders would require setting a
// zookeeper watch on every partition state node, and I am not sure that's worth it.
func retrievePartitionLeaders(partitions kazoo.PartitionList) partitionLeaderList {
	pls := make(partitionLeaderList, 0, len(partitions))
	for _, partition := range partitions {
		pl := partitionLeader{id: partition.ID, leader: partition.PreferredReplica(), partition: partition}
		pls = append(pls, pl)
	}
	return pls
}

// Divides a set of partitions between a set of consumers.
func distributePartitionsBetweenConsumers(consumers kazoo.ConsumergroupInstanceList, partitions partitionLeaderList) map[string][]*kazoo.Partition {
	result := make(map[string][]*kazoo.Partition)

	plen := len(partitions)
	clen := len(consumers)
	if clen == 0 {
		return result
	}

	sort.Sort(partitions)
	sort.Sort(consumers)

	n := plen / clen
	m := plen % clen
	p := 0
	for i, consumer := range consumers {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > plen {
			last = plen
		}

		for _, pl := range partitions[first:last] {
			result[consumer.ID] = append(result[consumer.ID], pl.partition)
		}
		p = last
	}

	return result
}

type partitionLeader struct {
	id        int32
	leader    int32
	partition *kazoo.Partition
}

// A sortable slice of PartitionLeader structs
type partitionLeaderList []partitionLeader

func (pls partitionLeaderList) Len() int {
	return len(pls)
}

func (pls partitionLeaderList) Less(i, j int) bool {
	return pls[i].leader < pls[j].leader || (pls[i].leader == pls[j].leader && pls[i].id < pls[j].id)
}

func (s partitionLeaderList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
