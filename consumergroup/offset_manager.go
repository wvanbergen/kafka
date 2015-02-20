package consumergroup

import (
	"errors"
	"sync"
	"time"
)

// OffsetManager is the main interface consumergroup requires to manage offsets of the consumergroup.
type OffsetManager interface {

	// InitializePartition is called when the consumergroup is starting to consume a
	// partition. It should return the last processed offset for this partition. Note:
	// the same partition can be initialized multiple times during a single run of a
	// consumer group due to other consumer instances coming online and offline.
	InitializePartition(topic string, partition int32) (int64, error)

	// MarkAsProcessed tells the offset manager than a certain message has been successfully
	// processed by the consumer, and should be committed. The implementation does not have
	// to store this offset right away, but should return true if it intends to do this at
	// some point.
	//
	// Offsets should generally be increasing if the consumer
	// processes events serially, but this cannot be guaranteed if the consumer does any
	// asynchronous processing. This can be handled in various ways, e.g. by only accepting
	// offsets that are higehr than the offsets seen before for the same partition.
	MarkAsProcessed(topic string, partition int32, offset int64) bool

	// FinalizePartition is called when the consumergroup is done consuming a
	// partition. In this method, the offset manager can flush any remaining offsets to its
	// backend store. It should return the last committed offset, and an error if there
	// was any problem flushing the offset. Note: it's possible that the consumergroup
	// instance will start to consume the same partition again after this function is called.
	FinalizePartition(topic string, partition int32) (int64, error)

	// Close is called when the consumergroup is shutting down. In normal circumstances, all
	// offsets are committed because FinalizePartition is called for all the running partition
	// consumers. You may want to check for this to be true, and try to commit any outstanding
	// offsets. If this doesn't succeed, it should return an error.
	Close() error
}

var (
	UncleanClose = errors.New("Not all offsets were committed before shutdown was completed.")
)

// OffsetManagerConfig holds configuration setting son how the offset manager should behave.
type OffsetManagerConfig struct {
	CommitInterval time.Duration // Interval between offset flushes to the backend store.
	VerboseLogging bool          // Whether to enable verbose logging.
}

// NewOffsetManagerConfig returns a new OffsetManagerConfig with sane defaults.
func NewOffsetManagerConfig() *OffsetManagerConfig {
	return &OffsetManagerConfig{
		CommitInterval: 10 * time.Second,
	}
}

type (
	topicOffsets    map[int32]*partitionOffsetTracker
	offsetsMap      map[string]topicOffsets
	offsetCommitter func(int64) error
)

type partitionOffsetTracker struct {
	l                      sync.Mutex
	highestProcessedOffset int64
	lastCommittedOffset    int64
}

type zookeeperOffsetManager struct {
	config  *OffsetManagerConfig
	l       sync.RWMutex
	offsets offsetsMap
	cg      *ConsumerGroup

	closing, closed chan struct{}
}

// NewZookeeperOffsetManager returns an offset manager that uses Zookeeper
// to store offsets.
func NewZookeeperOffsetManager(cg *ConsumerGroup, config *OffsetManagerConfig) OffsetManager {
	if config == nil {
		config = NewOffsetManagerConfig()
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

func (zom *zookeeperOffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	if zom.offsets[topic] == nil {
		zom.offsets[topic] = make(topicOffsets)
	}

	nextOffset, err := zom.cg.zk.Offset(zom.cg.name, topic, partition)
	if err != nil {
		return 0, err
	}

	zom.offsets[topic][partition] = &partitionOffsetTracker{
		highestProcessedOffset: nextOffset - 1,
		lastCommittedOffset:    nextOffset - 1,
	}

	return nextOffset, nil
}

func (zom *zookeeperOffsetManager) FinalizePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	tracker := zom.offsets[topic][partition]
	err := zom.commitOffset(topic, partition, tracker)
	if err == nil {
		delete(zom.offsets[topic], partition)
	}

	return tracker.lastCommittedOffset, err
}

func (zom *zookeeperOffsetManager) MarkAsProcessed(topic string, partition int32, offset int64) bool {
	zom.l.RLock()
	defer zom.l.RUnlock()
	return zom.offsets[topic][partition].markAsProcessed(offset)
}

func (zom *zookeeperOffsetManager) Close() error {
	close(zom.closing)
	<-zom.closed

	zom.l.Lock()
	defer zom.l.Unlock()

	var uncleanClose bool
	for topic, partitionOffsets := range zom.offsets {
		if len(partitionOffsets) > 0 {
			uncleanClose = true
			zom.cg.Logf("Uncommitted offsets for topic %s! Committing now...\n", topic)
		}
	}

	if uncleanClose {
		return UncleanClose
	} else {
		return nil
	}
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
			err := zom.commitOffset(topic, partition, offsetTracker)
			switch err {
			case nil:
				// noop
			default:
				returnErr = err
			}
		}
	}
	return returnErr
}

func (zom *zookeeperOffsetManager) commitOffset(topic string, partition int32, tracker *partitionOffsetTracker) error {
	err := tracker.commit(func(offset int64) error {
		return zom.cg.zk.Commit(zom.cg.name, topic, partition, offset+1)
	})

	if err != nil {
		zom.cg.Logf("FAILED to commit offset %d for %s/%d!", tracker.highestProcessedOffset, topic, partition)
	} else if zom.config.VerboseLogging {
		zom.cg.Logf("Committed offset %d for %s/%d!", tracker.lastCommittedOffset, topic, partition)
	}

	return err
}

// MarkAsProcessed marks the provided offset as highest processed offset if
// it's higehr than any previous offset it has received.
func (pot *partitionOffsetTracker) markAsProcessed(offset int64) bool {
	pot.l.Lock()
	defer pot.l.Unlock()
	if offset > pot.highestProcessedOffset {
		pot.highestProcessedOffset = offset
		return true
	} else {
		return false
	}
}

// Commit calls a committer function if the highest processed offset is out
// of sync with the last committed offset.
func (pot *partitionOffsetTracker) commit(committer offsetCommitter) error {
	pot.l.Lock()
	defer pot.l.Unlock()

	if pot.highestProcessedOffset > pot.lastCommittedOffset {
		if err := committer(pot.highestProcessedOffset); err != nil {
			return err
		}
		pot.lastCommittedOffset = pot.highestProcessedOffset
		return nil
	} else {
		return nil
	}
}
