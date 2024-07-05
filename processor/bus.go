package processor

import "github.com/IBM/sarama"

type topicAndPartition struct {
	Topic     string
	Partition int32
}

type MessageBus map[topicAndPartition]chan []*sarama.ProducerMessage

func (mb MessageBus) Send(partition int32, topic string, messages []*sarama.ProducerMessage) {
	mb[topicAndPartition{Partition: partition, Topic: topic}] <- messages
}
