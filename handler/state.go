package handler

import (
	"github.com/IBM/sarama"
)

type consumeState struct {
	badProcessingCount int
	buffer             []*sarama.ProducerMessage
	firstMessageBuffer *sarama.ConsumerMessage
	currMessage        *sarama.ConsumerMessage
	session            sarama.ConsumerGroupSession
	claim              sarama.ConsumerGroupClaim
}

func newConsumeState(bufferSize int, session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) *consumeState {
	return &consumeState{
		badProcessingCount: 0,
		buffer:             make([]*sarama.ProducerMessage, 0, bufferSize),
		session:            session,
		claim:              claim,
	}
}

func (cs *consumeState) shouldSendBuffer(bufferSize int) bool {
	return len(cs.buffer) >= bufferSize ||
		cs.claim.HighWaterMarkOffset()-cs.currMessage.Offset < int64(bufferSize/10) //towards when no more messages wierd.
}

func (cs *consumeState) reset() {
	cs.badProcessingCount = 0
	cs.buffer = cs.buffer[:0]
}
