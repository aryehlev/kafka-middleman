package handler

import (
	"github.com/IBM/sarama"
	"github.com/alitto/pond"
	"log"
)

type Consumer struct {
	ConsumerGroup string

	client sarama.ConsumerGroup

	handler sarama.ConsumerGroupHandler

	processingWorkers pond.WorkerPool
}

type Handler struct {
	ready   chan bool
	groupId string

	buffer []*sarama.ConsumerMessage

	messageSendingChannel chan []*sarama.ConsumerMessage
}

func (h Handler) Setup(session sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			h.messageSendingChannel <- []*sarama.ConsumerMessage{message}
		case <-session.Context().Done():
			return nil
		}
	}

}
