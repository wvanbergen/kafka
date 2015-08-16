package kafkaconsumer

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
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
		allChanges    = make(chan zk.Event, 1)
	)

	for topicName, _ := range ss.Subscription {
		topic := kz.Topic(topicName)

		if exists, err := topic.Exists(); err != nil {
			return nil, nil, err
		} else if !exists {
			sarama.Logger.Printf("Attempted to subscribe to %s, but this topic does not appear to exist. Ignoring...", topic.Name)
			continue
		}

		partitions, changes, err := topic.WatchPartitions()
		if err != nil {
			return nil, nil, err
		}

		go func(changes <-chan zk.Event) {
			event := <-changes
			allChanges <- event
		}(changes)

		for _, partition := range partitions {
			allPartitions = append(allPartitions, partition)
		}
	}

	return allPartitions, allChanges, nil
}

func (ss *staticSubscription) JSON() ([]byte, error) {
	return json.Marshal(ss)
}
