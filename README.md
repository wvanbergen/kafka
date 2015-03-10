# kafka

Kafka libraries, tools and example applications built on top of the
[sarama](https://github.com/Shopify/sarama) package.

### Libraries

- **consumergroup**: Distributed Kafka consumer, backed by Zookeeper, supporting load balancing and offset persistence, as defined by the [Kafka documentation](https://kafka.apache.org/documentation.html#distributionimpl). API documentation can be found on [godoc.org](http://godoc.org/github.com/wvanbergen/kafka/consumergroup)

### Tools

- **tools/stressproducer**: A tool to stress test the producer to measure throughput and latency.

### Examples

- **examples/consumergroup**: An example consumer application that uses the `consumergroup` library mentioned above.
