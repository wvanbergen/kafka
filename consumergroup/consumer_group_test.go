package consumergroup

import (
	"github.com/wvanbergen/kafka/kazoo"
	"testing"
)

func Test_PartitionDivision(t *testing.T) {

	consumers := []string{
		"consumer1",
		"consumer2",
	}

	partitions := []partitionLeader{
		partitionLeader{id: 0, leader: 1, partition: &kazoo.Partition{ID: 0}},
		partitionLeader{id: 1, leader: 2, partition: &kazoo.Partition{ID: 1}},
		partitionLeader{id: 2, leader: 1, partition: &kazoo.Partition{ID: 2}},
		partitionLeader{id: 3, leader: 2, partition: &kazoo.Partition{ID: 3}},
	}

	division := dividePartitionsBetweenConsumers(consumers, partitions)

	if len(division["consumer1"]) != 2 || division["consumer1"][0].ID != 0 || division["consumer1"][1].ID != 2 {
		t.Error("Consumer 1 should end up with partition 0 and 2")
	}

	if len(division["consumer2"]) != 2 || division["consumer2"][0].ID != 1 || division["consumer2"][1].ID != 3 {
		t.Error("Consumer 2 should end up with partition 1 and 3")
	}
}
