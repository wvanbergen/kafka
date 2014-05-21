package kafkaconsumer

import (
	"errors"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

const (
	REBALANCE_START uint8 = iota + 1
	REBALANCE_OK
	REBALANCE_ERROR
)

type KafkaConsumer struct {
	Name          string
	Zookeeper     *ZK
	Kafka         *sarama.Client
	ConsumerGroup *ConsumerGroup
	stopper       chan bool
	done          chan bool
}

var (
	clientConfig   = sarama.ClientConfig{MetadataRetries: 5, WaitForElection: 250 * time.Millisecond}
	consumerConfig = sarama.ConsumerConfig{MaxWaitTime: 500000, DefaultFetchSize: 256 * 1024, MinFetchSize: 1024, OffsetMethod: sarama.OffsetMethodOldest}
)

func NewKafkaConsumer(name string, zookeeper []string) (*KafkaConsumer, error) {
	if len(zookeeper) == 0 {
		return nil, errors.New("You need to provide at least one zookeeper node address!")
	}

	consumer := &KafkaConsumer{Name: name}
	if zk, err := NewZK(zookeeper, 1*time.Second); err != nil {
		return nil, err
	} else {
		consumer.Zookeeper = zk
	}

	if kafkaBrokers, err := consumer.Zookeeper.Brokers(); err != nil {
		return nil, err
	} else if client, err := sarama.NewClient(name, kafkaBrokers, &clientConfig); err != nil {
		return nil, err
	} else {
		consumer.Kafka = client
	}

	consumer.stopper = make(chan bool)
	consumer.done = make(chan bool)
	return consumer, nil
}

func (kc *KafkaConsumer) Close() error {
	close(kc.stopper)
	<-kc.done

	if kc.ConsumerGroup != nil {
		kc.ConsumerGroup.Close()
	}

	if kc.Kafka != nil {
		kc.Kafka.Close()
	}

	if kc.Zookeeper != nil {
		kc.Zookeeper.Close()
	}

	return nil
}

func (kc *KafkaConsumer) Stream(topic string) (chan *Event, error) {

	cg, cgErr := NewConsumerGroup(kc.Kafka, kc.Zookeeper, kc.Name, topic, nil, &consumerConfig)
	if cgErr != nil {
		return nil, cgErr
	}

	topicChannel := make(chan *Event)

	go func() {
		for {
			select {
			case <-kc.stopper:
				cg.Close()
				close(topicChannel)
				close(kc.done)
				return

			default:
				cg.Checkout(func(pc *PartitionConsumer) error {
					log.Printf("Start consuming partition %d...", pc.partition)

					partitionChannel := make(chan *Event)
					partitionStopper := make(chan bool)

					go func() {
						if err := pc.Fetch(partitionChannel, 1*time.Second); err != nil {
							log.Println("Fetch failed", err)
						}

						close(partitionStopper)
					}()

					var offset int64
					for {
						select {
						case <-partitionStopper:
							log.Printf("Last seen offset %d", offset)
							return nil

						case event, ok := <-partitionChannel:
							if ok {
								offset = event.Offset
								topicChannel <- event
							} else {
								return errors.New("Failed to read event from channel!")
							}
						}
					}
				})
			}

		}
	}()

	return topicChannel, nil
}

type Event struct {
	sarama.ConsumerEvent
	Topic     string
	Partition int32
}

// COMMON TYPES

// Partition information
type Partition struct {
	Id   int32
	Addr string // Leader address
}

// A sortable slice of Partition structs
type PartitionSlice []Partition

func (s PartitionSlice) Len() int { return len(s) }
func (s PartitionSlice) Less(i, j int) bool {
	if s[i].Addr < s[j].Addr {
		return true
	}
	return s[i].Id < s[j].Id
}
func (s PartitionSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// A subscribable notification
type Notification struct {
	Type uint8
	Src  *ConsumerGroup
	Err  error
}
