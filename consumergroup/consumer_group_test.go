package consumergroup

import (
	"testing"
)

func Test_PartitionDivision(t *testing.T) {

	consumers := []string{
		"consumer1",
		"consumer2",
	}

	partitions := partitionLeaderSlice{
		partitionLeader{id: 0, leader: "broker1"},
		partitionLeader{id: 1, leader: "broker2"},
		partitionLeader{id: 2, leader: "broker1"},
		partitionLeader{id: 3, leader: "broker2"},
	}

	division := dividePartitionsBetweenConsumers(consumers, partitions)

	if len(division["consumer1"]) != 2 || division["consumer1"][0].id != 0 || division["consumer1"][1].id != 2 {
		t.Error("Consumer 1 should end up with partition 0 and 2")
	}

	if len(division["consumer2"]) != 2 || division["consumer2"][0].id != 1 || division["consumer2"][1].id != 3 {
		t.Error("Consumer 2 should end up with partition 1 and 3")
	}
}
