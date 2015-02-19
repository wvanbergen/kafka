package consumergroup

import (
	"sync"
	"time"
)

type OffsetManager interface {
	InitializePartition(topic string, partition int32) (int64, error)
	MarkAsSeen(topic string, partition int32, offset int64) bool
	Close() error
}

type OffsetManagerConfig struct {
	CommitInterval time.Duration
	VerboseLogging bool
}

type zookeeperOffsetManager struct {
	config  *OffsetManagerConfig
	l       sync.RWMutex
	offsets offsetsMap
	cg      *ConsumerGroup

	closing, closed chan struct{}
}

func NewZookeeperOffsetManager(cg *ConsumerGroup, config *OffsetManagerConfig) OffsetManager {
	if config == nil {
		config = &OffsetManagerConfig{CommitInterval: 10 * time.Second}
	}

	zom := &zookeeperOffsetManager{
		config:  config,
		cg:      cg,
		offsets: make(offsetsMap),
		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}

	go zom.offsetCommitter()

	return zom
}

func (zom *zookeeperOffsetManager) MarkAsSeen(topic string, partition int32, offset int64) bool {
	zom.l.RLock()
	defer zom.l.RUnlock()
	return zom.offsets[topic][partition].MarkAsSeen(offset)
}

func (zom *zookeeperOffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	if zom.offsets[topic] == nil {
		zom.offsets[topic] = make(topicOffsets)
	}

	nextOffset, err := zom.cg.zk.Offset(zom.cg.name, topic, partition)
	if err != nil {
		return -1, err
	}
	zom.offsets[topic][partition] = newPartitionOffsetTracker(nextOffset - 1)

	return nextOffset, nil
}

func (zom *zookeeperOffsetManager) Close() error {
	close(zom.closing)
	<-zom.closed
	return zom.commitOffsets()
}

func (zom *zookeeperOffsetManager) offsetCommitter() {
	commitTicker := time.NewTicker(zom.config.CommitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-zom.closing:
			close(zom.closed)
			return
		case <-commitTicker.C:
			zom.commitOffsets()
		}
	}
}

func (zom *zookeeperOffsetManager) commitOffsets() error {
	zom.l.RLock()
	defer zom.l.RUnlock()

	var returnErr error
	for topic, partitionOffsets := range zom.offsets {
		for partition, offsetTracker := range partitionOffsets {
			committer := func(offset int64) error {
				return zom.cg.zk.Commit(zom.cg.name, topic, partition, offset+1)
			}

			err := offsetTracker.Commit(committer)
			switch err {
			case nil:
				if zom.config.VerboseLogging {
					zom.cg.Logf("Committed offset %d for %s:%d\n", offsetTracker.highestOffsetSeen, topic, partition)
				}
			case AlreadyCommitted:
				// noop
			default:
				returnErr = err
				zom.cg.Logf("Failed to commit offset %d for %s:%d\n", offsetTracker.highestOffsetSeen, topic, partition)
			}
		}
	}
	return returnErr
}

type offsetCommitter func(int64) error
type topicOffsets map[int32]*partitionOffsetTracker
type offsetsMap map[string]topicOffsets

type partitionOffsetTracker struct {
	l                   sync.Mutex
	highestOffsetSeen   int64
	lastCommittedOffset int64
}

func newPartitionOffsetTracker(lastCommittedOffset int64) *partitionOffsetTracker {
	return &partitionOffsetTracker{lastCommittedOffset: lastCommittedOffset}
}

func (pot *partitionOffsetTracker) MarkAsSeen(offset int64) bool {
	pot.l.Lock()
	defer pot.l.Unlock()
	if offset > pot.highestOffsetSeen {
		pot.highestOffsetSeen = offset
		return true
	} else {
		return false
	}
}

func (pot *partitionOffsetTracker) Commit(committer offsetCommitter) error {
	pot.l.Lock()
	defer pot.l.Unlock()

	if pot.highestOffsetSeen > pot.lastCommittedOffset {
		if err := committer(pot.highestOffsetSeen); err != nil {
			return err
		}
		pot.lastCommittedOffset = pot.highestOffsetSeen
	}
	return nil
}
