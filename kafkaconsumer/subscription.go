package kafkaconsumer

import (
	"encoding/json"
	"regexp"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

type SubscriptionPattern string

const (
	SubscriptionPatternStatic    SubscriptionPattern = "static"
	SubscriptionPatternWhiteList SubscriptionPattern = "white_list"
	SubscriptionPatternBlackList SubscriptionPattern = "black_list"
)

// Subscription describes what topics/partitions a consumer instance is
// subscribed to. This can be a static list of topic, or can be a regular
// expression that acts as a whitelist or blacklist of topics.
//
// The subscription is responsible for watching zookeeper to changes is
// the list of topics or partitions, and notify the consumer so it
// can trigger a rebalance.
type Subscription interface {

	// WatchPartitions returns a list of partitions that the consumer group should
	// consume, and a channel that will be fired if this list has changed.
	WatchPartitions(kazoo *kazoo.Kazoo) (kazoo.PartitionList, <-chan zk.Event, error)

	// JSON returns a JSON-encoded representation of the subscription, which will be
	// stored in Zookeeper alongside every running instance registration.
	JSON() ([]byte, error)
}

type staticSubscription struct {
	Pattern      SubscriptionPattern `json:"pattern"`
	Subscription map[string]int      `json:"subscription"`
	Timestamp    int64               `json:"timestamp,string"`
	Version      int                 `json:"version"`
}

// TopicSubscription creates a static subscription for a list of topics.
func TopicSubscription(topics ...string) Subscription {
	sub := make(map[string]int)
	for _, topic := range topics {
		sub[topic] = 1
	}
	return StaticSubscription(sub)
}

// StaticSubscription creates a static subscription for a map of topics,
// and the number of streams that will be used to consume it.
func StaticSubscription(subscription map[string]int) Subscription {
	return &staticSubscription{
		Version:      1,
		Timestamp:    time.Now().UnixNano() / int64(time.Millisecond),
		Pattern:      SubscriptionPatternStatic,
		Subscription: subscription,
	}
}

func (ss *staticSubscription) WatchPartitions(kz *kazoo.Kazoo) (kazoo.PartitionList, <-chan zk.Event, error) {
	var (
		allPartitions = make([]*kazoo.Partition, 0)
		ca            = newChangeAggregator()
	)

	for topicName, _ := range ss.Subscription {
		topic := kz.Topic(topicName)

		if exists, err := topic.Exists(); err != nil {
			return nil, nil, err
		} else if !exists {
			Logger.Printf("Attempted to subscribe to %s, but this topic does not appear to exist. Ignoring...", topic.Name)
			continue
		}

		partitions, partitionsChanged, err := topic.WatchPartitions()
		if err != nil {
			return nil, nil, err
		}

		ca.handleChange(partitionsChanged)

		for _, partition := range partitions {
			allPartitions = append(allPartitions, partition)
		}
	}

	return allPartitions, ca.channel(), nil
}

// JSON returns the json representation of the static subscription
func (ss *staticSubscription) JSON() ([]byte, error) {
	return json.Marshal(ss)
}

type regexpSubscription struct {
	Pattern      SubscriptionPattern `json:"pattern"`
	Subscription map[string]int      `json:"subscription"`
	Timestamp    int64               `json:"timestamp,string"`
	Version      int                 `json:"version"`

	regexp *regexp.Regexp
}

// WhitelistSubscription creates a subscription on topics that match a given regular expression.
// It will automatically subscribe to new topics that match the pattern when they are created.
func WhitelistSubscription(pattern *regexp.Regexp) Subscription {
	subscription := make(map[string]int)
	subscription[pattern.String()] = 1

	return &regexpSubscription{
		Version:      1,
		Timestamp:    time.Now().UnixNano() / int64(time.Millisecond),
		Pattern:      SubscriptionPatternWhiteList,
		Subscription: subscription,

		regexp: pattern,
	}
}

// BlacklistSubscription creates a subscription on topics that do not match a given regular expression.
// It will automatically subscribe to new topics that don't match the pattern when they are created.
func BlacklistSubscription(pattern *regexp.Regexp) Subscription {
	subscription := make(map[string]int)
	subscription[pattern.String()] = 1

	return &regexpSubscription{
		Version:      1,
		Timestamp:    time.Now().UnixNano() / int64(time.Millisecond),
		Pattern:      SubscriptionPatternBlackList,
		Subscription: subscription,

		regexp: pattern,
	}
}

func (rs *regexpSubscription) topicMatchesPattern(topicName string) bool {
	switch rs.Pattern {
	case SubscriptionPatternWhiteList:
		return rs.regexp.MatchString(topicName)
	case SubscriptionPatternBlackList:
		return !rs.regexp.MatchString(topicName)
	default:
		panic("Unexpected pattern for regexpSubscription")
	}
}

func (rs *regexpSubscription) WatchPartitions(kz *kazoo.Kazoo) (kazoo.PartitionList, <-chan zk.Event, error) {
	var (
		allPartitions = make([]*kazoo.Partition, 0)
		ca            = newChangeAggregator()
	)

	topics, topicsChanged, err := kz.WatchTopics()
	if err != nil {
		return nil, nil, err
	}

	ca.handleChange(topicsChanged)

	for _, topic := range topics {
		if rs.topicMatchesPattern(topic.Name) {
			partitions, partitionsChanged, err := topic.WatchPartitions()
			if err != nil {
				return nil, nil, err
			}

			ca.handleChange(partitionsChanged)

			for _, partition := range partitions {
				allPartitions = append(allPartitions, partition)
			}
		}
	}

	return allPartitions, ca.channel(), nil
}

// JSON returns the json representation of the static subscription
func (rs *regexpSubscription) JSON() ([]byte, error) {
	return json.Marshal(rs)
}

func newChangeAggregator() *changeAggregator {
	return &changeAggregator{
		dying: make(chan struct{}, 0),
		c:     make(chan zk.Event, 1),
	}
}

// changeAggregator will emit only the first change event to the
// output channel. All other goroutines waiting for zookeeper watches
// will be stopped once this happens.
type changeAggregator struct {
	dying chan struct{}
	c     chan zk.Event
	once  sync.Once
}

func (ca *changeAggregator) handleChange(change <-chan zk.Event) {
	go ca.waitForChange(change)
}

func (ca *changeAggregator) waitForChange(change <-chan zk.Event) {
	select {
	case <-ca.dying:
		return
	case event := <-change:
		ca.once.Do(func() {
			close(ca.dying)
			ca.c <- event
			close(ca.c)
		})
	}
}

func (ca *changeAggregator) channel() <-chan zk.Event {
	return ca.c
}
