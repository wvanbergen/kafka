package kafkaconsumer

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

type Config struct {
	*sarama.Config

	Zookeeper *kazoo.Config

	MaxProcessingTime time.Duration // Time to wait for all the offsets for a partition to be processed after stopping to consume from it. Defaults to 1 minute.

	Offsets struct {
		Initial int64 // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetOldest (default) or sarama.OffsetNewest.
	}
}

func NewConfig() *Config {
	config := &Config{}
	config.Config = sarama.NewConfig()
	config.Zookeeper = kazoo.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.MaxProcessingTime = 60 * time.Second

	return config
}

func (cgc *Config) Validate() error {
	if cgc.Zookeeper.Timeout <= 0 {
		return sarama.ConfigurationError("Zookeeper.Timeout should have a duration > 0")
	}

	if cgc.MaxProcessingTime <= 0 {
		return sarama.ConfigurationError("MaxProcessingTime should have a duration > 0")
	}

	if cgc.Offsets.Initial != sarama.OffsetOldest && cgc.Offsets.Initial != sarama.OffsetNewest {
		return errors.New("Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest.")
	}

	if cgc.Config != nil {
		if err := cgc.Config.Validate(); err != nil {
			return err
		}
	}

	return nil
}
