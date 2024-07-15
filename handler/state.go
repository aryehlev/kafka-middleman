package handler

import (
	"github.com/IBM/sarama"
	"time"
)

type consumeState struct {
	badProcessingCount int
	buffer             []*sarama.ProducerMessage
	firstMessageBuffer *sarama.ConsumerMessage
	currMessage        *sarama.ConsumerMessage
	session            sarama.ConsumerGroupSession
	claim              sarama.ConsumerGroupClaim
	lastSendTime       time.Time
}

func NewConsumeState(bufferSize int, session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) *consumeState {
	return &consumeState{
		badProcessingCount: 0,
		buffer:             make([]*sarama.ProducerMessage, 0, bufferSize),
		session:            session,
		claim:              claim,
	}
}

func (cs *consumeState) shouldSendBuffer(bufferSize int, maxBufferTime time.Duration) bool {
	return len(cs.buffer) >= bufferSize ||
		time.Since(cs.lastSendTime) >= maxBufferTime
}

func (cs *consumeState) reset() {
	cs.badProcessingCount = 0
	cs.buffer = cs.buffer[:0]
	cs.lastSendTime = time.Now()
}
