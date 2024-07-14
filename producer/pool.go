package producer

import (
	"errors"
	"sync"

	"github.com/IBM/sarama"
)

type topicPartition struct {
	topic     string
	partition int32
}

type Pool struct {
	workers    map[topicPartition][]*Producer
	New        func(topic string, partition int32) (*Producer, error)
	mutex      *sync.Mutex
	NumRetries int
}

func (p *Pool) Send(topic string, partition int32, messages []*sarama.ProducerMessage, groupId string,
	offsets map[string][]*sarama.PartitionOffsetMetadata) error {
	worker, err := p.get(topic, partition)
	if err != nil {
		return err
	}

	for i := 0; i < p.NumRetries && errors.Is(err, BadProducerError); i++ {
		worker.Close()
		worker, err := p.get(topic, partition)
		if err != nil {
			return err
		}

		err = worker.run(messages, groupId, offsets)
	}

	if !errors.Is(err, BadProducerError) {
		p.put(topic, partition, worker)
	} else {
		worker.Close()
	}

	return err
}

func (p *Pool) get(topic string, partition int32) (*Producer, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	tp := topicPartition{topic: topic, partition: partition}
	if producers, ok := p.workers[tp]; !ok || len(producers) == 0 {
		return p.New(topic, partition)
	}

	index := len(p.workers[tp]) - 1
	worker := p.workers[tp][index]
	p.workers[tp] = p.workers[tp][:index]

	return worker, nil
}

func (p *Pool) put(topic string, partition int32, worker *Producer) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	tp := topicPartition{topic: topic, partition: partition}
	p.workers[tp] = append(p.workers[tp], worker)
}

// Input: map of topics to partitions.
func (p *Pool) Init(claims map[string][]int32) error {
	for topic, partitions := range claims {
		for _, partition := range partitions {
			if err := p.add(topic, partition); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Pool) add(topic string, partition int32) error {
	tp := topicPartition{topic: topic, partition: partition}
	if _, ok := p.workers[tp]; !ok { //only add if dosnt exist.
		worker, err := p.New(topic, partition)
		if err != nil {
			return err
		}
		p.workers[tp] = append(p.workers[tp], worker)
	}

	return nil
}
