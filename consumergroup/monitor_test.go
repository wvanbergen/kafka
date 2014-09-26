package consumergroup

import (
	"log"
)

func ExampleConsumerGroupMonitoring() {
	monitor, err := NewMonitor("sample_monitor", "sample_consumer", []string{"zookeeper1.local:2181", "zookeeper2.local:2181"})
	if err != nil {
		log.Fatal(err)
	}

	eventsBehindLatest, err := monitor.Check()
	if err != nil {
		log.Fatal(err)
	}

	for topic, topicMap := range eventsBehindLatest {
		for partition, eventsBehind := range topicMap {
			log.Printf("topic=%s partition=%d events_behind=%d", topic, partition, eventsBehind)
		}
	}
}
