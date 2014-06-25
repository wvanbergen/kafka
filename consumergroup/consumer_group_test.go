package consumergroup

import (
	"log"
	"os"
	"os/signal"
)

func ExampleConsumerGroup() {
	consumerGroup := "my_consumer_group_name"
	kafkaTopic := "my_topic"
	zookeeper := []string{"localhost:2181"}

	consumer, consumerErr := JoinConsumerGroup(consumerGroup, kafkaTopic, zookeeper, nil)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		consumer.Close()
	}()

	eventCount := 0
	offsets := make(map[int32]int64)

	stream := consumer.Stream()
	for {
		event, ok := <-stream
		if !ok {
			break
		}

		eventCount += 1
		if offsets[event.Partition] != 0 && offsets[event.Partition] != event.Offset-1 {
			log.Printf("Unexpected offset on partition %d: %d. Expected %d\n", event.Partition, event.Offset, offsets[event.Partition]+1)
		}

		offsets[event.Partition] = event.Offset
	}

	log.Printf("Processed %d events.", eventCount)
}
