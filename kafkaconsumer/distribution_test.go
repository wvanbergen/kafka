package kafkaconsumer

import (
	"fmt"
	"github.com/wvanbergen/kazoo-go"
	"sort"
	"testing"
)

func TestPartitionLeaderList(t *testing.T) {
	var (
		topicFoo = &kazoo.Topic{Name: "foo"}
		topicBar = &kazoo.Topic{Name: "bar"}
	)

	var (
		partitionFoo_1 = topicFoo.Partition(1, []int32{3, 2, 1})
		partitionFoo_2 = topicFoo.Partition(2, []int32{1, 3, 1})
		partitionBar_1 = topicBar.Partition(1, []int32{3, 1, 2})
		partitionBar_2 = topicBar.Partition(2, []int32{3, 2, 1})
	)

	partitions := kazoo.PartitionList{partitionFoo_1, partitionFoo_2, partitionBar_1, partitionBar_2}
	partitionLeaders := retrievePartitionLeaders(partitions)
	sort.Sort(partitionLeaders)

	if partitionLeaders[0].partition != partitionFoo_2 || partitionLeaders[1].partition != partitionBar_1 || partitionLeaders[2].partition != partitionBar_2 || partitionLeaders[3].partition != partitionFoo_1 {
		t.Error("Unexpected order after sorting partition leader list", partitionLeaders)
	}
}

func TestPartitionDistribution(t *testing.T) {
	type testCase struct{ consumers, partitions int }

	consumerPartitionTestCases := []testCase{
		testCase{consumers: 0, partitions: 1},
		testCase{consumers: 1, partitions: 0},
		testCase{consumers: 2, partitions: 5},
		testCase{consumers: 5, partitions: 2},
		testCase{consumers: 9, partitions: 32},
		testCase{consumers: 10, partitions: 50},
		testCase{consumers: 232, partitions: 592},
	}

	for _, tc := range consumerPartitionTestCases {
		var (
			consumers  = createTestConsumerGroupInstanceList(tc.consumers)
			partitions = createTestPartitions(tc.partitions)
		)

		distribution := distributePartitionsBetweenConsumers(consumers, partitions)

		var (
			grouping    = make(map[int32]struct{})
			maxConsumed = 0
			minConsumed = len(partitions) + 1
		)

		for _, v := range distribution {
			if len(v) > maxConsumed {
				maxConsumed = len(v)
			}
			if len(v) < minConsumed {
				minConsumed = len(v)
			}
			for _, partition := range v {
				if _, ok := grouping[partition.ID]; ok {
					t.Errorf("PartitionDivision: Partition %v was assigned more than once!", partition.ID)
				} else {
					grouping[partition.ID] = struct{}{}
				}
			}
		}

		if len(consumers) > 0 {
			if len(grouping) != len(partitions) {
				t.Errorf("PartitionDivision: Expected to divide %d partitions among consumers, but only %d partitions were consumed.", len(partitions), len(grouping))
			}
			if (maxConsumed - minConsumed) > 1 {
				t.Errorf("PartitionDivision: Partitions weren't divided evenly, consumers shouldn't have a difference of more than 1 in the number of partitions consumed (was %d).", maxConsumed-minConsumed)
			}
			if minConsumed > 1 && len(consumers) != len(distribution) {
				t.Errorf("PartitionDivision: Partitions weren't divided evenly, some consumers didn't get any paritions even though there were %d partitions and %d consumers.", len(partitions), len(consumers))
			}
		} else {
			if len(grouping) != 0 {
				t.Error("PartitionDivision: Expected to not distribute partitions without running instances")
			}
		}
	}
}

func createTestConsumerGroupInstanceList(size int) kazoo.ConsumergroupInstanceList {
	k := make(kazoo.ConsumergroupInstanceList, size)
	for i := range k {
		k[i] = &kazoo.ConsumergroupInstance{ID: fmt.Sprintf("consumer%d", i)}
	}
	return k
}

func createTestPartitions(count int) []partitionLeader {
	topic := &kazoo.Topic{Name: "foo"}
	p := make([]partitionLeader, count)
	for i := range p {
		p[i] = partitionLeader{id: int32(i), leader: 1, partition: topic.Partition(int32(i), []int32{1, 2, 3})}
	}
	return p
}
