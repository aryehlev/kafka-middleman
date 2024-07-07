package handler

import (
	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/processor"
	"github.com/aryehlev/kafka-middleman/producer"
	"log"
)

type Handler[T, S any] struct {
	processor processor.Worker[T, S]
	sink      producer.Worker

	errorHandler func(*sarama.ConsumerMessage, error)
	groupId      string
}

func (h *Handler[_, _]) Setup(session sarama.ConsumerGroupSession) error {
	h.sink = producer.Worker{}
	return nil
}

func (h *Handler[_, _]) Cleanup(session sarama.ConsumerGroupSession) error {
	return producer.Worker.Close()
}

func (h *Handler[_, _]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			err := h.processor.Run(message)
			if err != nil {
				h.errorHandler(message, err)
				continue
			}

			restart, reset := h.sink.Run(message, h.groupId)
			if reset {
				session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			}
			if restart {
				err := h.sink.Restart()
				h.errorHandler(message, err)
				continue
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
