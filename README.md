# kafka

Kafka tools and examples built on top of the sarama package.

## Packages

- **consumergroup**: Distributed Kafka consumer, backed by Zookeeper, supporting load balancing and offset persistence, as defined by the Kafka documentation: https://kafka.apache.org/documentation.html#distributionimpl.


## Examples

- **topic_consumer.go** ConsumerGroup example.
- **monitor.go** Example app to monitior how far behind a ConsumerGroup has fallen on each topic/partition
