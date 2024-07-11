package handler

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/processor"
	"github.com/aryehlev/kafka-middleman/producer"
)

type Handler[T, S any] struct {
	processor processor.Worker[T, S]
	sink      *producer.Worker

	groupId string

	buffer     []*sarama.ProducerMessage
	bufferSize int

	destTopic    string
	producerConf *sarama.Config
	addrs        []string
}

func New[T, S any](groupId string, bufferSize int, destTopic string, producerConf *sarama.Config, addrs []string) Handler[T, S] {
	return Handler[T, S]{
		processor:    processor.Worker[T, S]{},
		groupId:      groupId,
		buffer:       make([]*sarama.ProducerMessage, 0, bufferSize),
		bufferSize:   bufferSize,
		destTopic:    destTopic,
		producerConf: producerConf,
		addrs:        addrs,
	}
}

func (h *Handler[_, _]) Setup(session sarama.ConsumerGroupSession) error {
	var err error
	h.sink, err = producer.New(h.destTopic, h.producerConf, h.addrs)

	return err
}

func (h *Handler[_, _]) Cleanup(session sarama.ConsumerGroupSession) error {
	return h.sink.Close()
}

func (h *Handler[_, _]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			produceMsg, err := h.processor.Run(message)
			if err != nil {
				session.MarkOffset(message.Topic, message.Partition, message.Offset+1, "")
				continue
			}

			h.buffer = append(h.buffer, produceMsg)

			if len(h.buffer) >= h.bufferSize {
				lowestMessage := h.buffer[0]
				offsets := make(map[string][]*sarama.PartitionOffsetMetadata)
				offsets[message.Topic] = []*sarama.PartitionOffsetMetadata{
					{
						Partition: message.Partition,
						Offset:    message.Offset + 1,
					},
				}
				err := h.sink.Run(h.buffer, h.groupId, offsets)
				switch err {
				case producer.BadMessagesError:
					session.ResetOffset(lowestMessage.Topic, lowestMessage.Partition, lowestMessage.Offset, "")
				case producer.BadProducerError:
					err := h.sink.Restart()
					if err != nil {
					}
					err = h.sink.Run(h.buffer, h.groupId, nil)
					if err != nil {
					}
				}

				h.buffer = h.buffer[:0]

			}

		case <-session.Context().Done():
			return nil
		}
	}
}
