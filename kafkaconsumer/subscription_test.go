package kafkaconsumer

import (
	"encoding/json"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

func TestStaticSubscriptionJSON(t *testing.T) {
	subscription := TopicSubscription("test1", "test2")
	bytes, err := subscription.JSON()
	if err != nil {
		t.Fatal(err)
	}

	data := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &data); err != nil {
		t.Fatal(err)
	}

	if data["pattern"] != "static" {
		t.Errorf("pattern should be set to static, but was %+v", data["pattern"])
	}

	if data["version"] != float64(1) {
		t.Errorf("version should be set to 1, but was %+v", data["version"])
	}

	if data["timestamp"] == nil || data["timestamp"] == "" {
		t.Errorf("timestamp should be set, but was %+v", data["timestamp"])
	}

	if s, ok := data["subscription"].(map[string]interface{}); ok {
		if len(s) != 2 {
			t.Error("Subscription should have 2 entries")
		}

		if s["test1"] != float64(1) {
			t.Error("Subscription for test1 was not properly set")
		}

		if s["test2"] != float64(1) {
			t.Error("Subscription for test2 was not properly set")
		}
	} else {
		t.Error("subscription should be a map")
	}
}

func TestRegexpSubscription(t *testing.T) {
	whitelist := WhitelistSubscription(regexp.MustCompile("^test\\..*")).(*regexpSubscription)

	if whitelist.Pattern != SubscriptionPatternWhiteList {
		t.Error("Subscription should have white_list pattern, but has:", whitelist.Pattern)
	}

	if !whitelist.topicMatchesPattern("test.1") {
		t.Error("Subscription should match topic test.1")
	}

	if whitelist.topicMatchesPattern("foo") {
		t.Error("Subscription should not match topic foo")
	}

	blacklist := BlacklistSubscription(regexp.MustCompile("^test\\..*")).(*regexpSubscription)

	if blacklist.Pattern != SubscriptionPatternBlackList {
		t.Error("Subscription should have black_list pattern, but has:", blacklist.Pattern)
	}

	if blacklist.topicMatchesPattern("test.1") {
		t.Error("Subscription should not match topic test.1")
	}

	if !blacklist.topicMatchesPattern("foo") {
		t.Error("Subscription should match topic foo")
	}
}

func TestRegexpSubscriptionJSON(t *testing.T) {
	subscription := WhitelistSubscription(regexp.MustCompile("^test\\..*"))
	bytes, err := subscription.JSON()
	if err != nil {
		t.Fatal(err)
	}

	data := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &data); err != nil {
		t.Fatal(err)
	}

	if data["pattern"] != "white_list" {
		t.Errorf("pattern should be set to static, but was %+v", data["pattern"])
	}

	if data["version"] != float64(1) {
		t.Errorf("version should be set to 1, but was %+v", data["version"])
	}

	if data["timestamp"] == nil || data["timestamp"] == "" {
		t.Errorf("timestamp should be set, but was %+v", data["timestamp"])
	}

	if s, ok := data["subscription"].(map[string]interface{}); ok {
		if len(s) != 1 && s["^test\\..*"] != float64(1) {
			t.Error("Pattern was not set properly in the Subscription field")
		}
	} else {
		t.Error("subscription should be a map")
	}
}

func TestChangeAggregator(t *testing.T) {

	var (
		changesHandled, changesSubmitted int32

		wg sync.WaitGroup
		ca = newChangeAggregator()
	)

	for i := 0; i < 100; i++ {
		change := make(chan zk.Event, 1)

		wg.Add(1)
		go func(change chan<- zk.Event) {
			defer wg.Done()
			change <- zk.Event{}
			close(change)
			atomic.AddInt32(&changesSubmitted, 1)
		}(change)

		wg.Add(1)
		go func(change <-chan zk.Event) {
			defer wg.Done()
			ca.waitForChange(change)
			atomic.AddInt32(&changesHandled, 1)
		}(change)
	}

	wg.Wait()

	if changesSubmitted != 100 || changesHandled != 100 {
		t.Errorf("Expected 100 changes to be submitted and handled, but found %d and %d", changesSubmitted, changesHandled)
	}

	if _, ok := <-ca.channel(); !ok {
		t.Errorf("Expected to read a single event from the output channel, but the channel is closed already")
	}

	if _, ok := <-ca.channel(); ok {
		t.Errorf("Expected the output channel to be closed afte reading the first event")
	}
}
