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

	partitions := []kazoo.Partition{
		kazoo.Partition{ID: 0, Leader: 1},
		kazoo.Partition{ID: 1, Leader: 2},
		kazoo.Partition{ID: 2, Leader: 1},
		kazoo.Partition{ID: 3, Leader: 2},
	}

	division := dividePartitionsBetweenConsumers(consumers, partitions)

	if len(division["consumer1"]) != 2 || division["consumer1"][0].ID != 0 || division["consumer1"][1].ID != 2 {
		t.Error("Consumer 1 should end up with partition 0 and 2")
	}

	if len(division["consumer2"]) != 2 || division["consumer2"][0].ID != 1 || division["consumer2"][1].ID != 3 {
		t.Error("Consumer 2 should end up with partition 1 and 3")
	}
}
