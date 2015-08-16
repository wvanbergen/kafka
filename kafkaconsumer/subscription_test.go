package kafkaconsumer

import (
	"encoding/json"
	"testing"
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
