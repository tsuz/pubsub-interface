package producer

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

type Producer interface {
	ProduceAsync(topic string, key []byte, msg interface{}) error
	ProduceSync(topic string, key []byte, msg interface{}) error
	Close()
}

func NewKafkaProducer(p *kafka.Producer) (Producer, error) {
	return &producer{
		k: p,
	}, nil
}

type producer struct {
	k *kafka.Producer
}

// ProduceAsync produces asynchronously
func (p *producer) ProduceAsync(topic string, key []byte, msg interface{}) error {

	payload, err := json.Marshal(msg)

	if err != nil {
		return errors.Wrap(err, "Error serializing payload")
	}

	return p.k.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
		Key:   key,
	}, nil)
}

// ProduceSync produces synchronously, blocking the thread
func (p *producer) ProduceSync(topic string, key []byte, msg interface{}) error {

	ch := make(chan kafka.Event, 1)

	payload, err := json.Marshal(msg)

	if err != nil {
		return errors.Wrap(err, "Error serializing payload")
	}

	if err := p.k.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
		Key:   key,
	}, ch); err != nil {
		return errors.Wrap(err, "Error producing")
	}

	// blocks thread
	e := <-ch

	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			return errors.Wrapf(ev.TopicPartition.Error, "Error producing msg %+v", msg)
		}
	}

	return nil
}

// Close flushes and closes producer chnanel
func (p *producer) Close() {
	if p.k != nil {
		p.k.Flush(15 * 1000)
		p.k.Close()
	}
}
