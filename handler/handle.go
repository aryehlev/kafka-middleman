package handler

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/processor"
	"github.com/aryehlev/kafka-middleman/producer"
)

type Handler[In, Out any] struct {
	processor         processor.Worker[In, Out]
	sinks             *producer.Pool
	topic             string
	groupId           string
	bufferSize        int
	maxProcessingTime time.Duration
	destTopic         string
	producerConf      sarama.Config
	addrs             []string
	allowedRetries    int
}

type Conf[In, Out any] struct {
	GroupId        string
	BufferSize     int
	ProcessingTime time.Duration
	DestTopic      string
	ProducerConf   sarama.Config
	Addrs          []string
	Worker         processor.Worker[In, Out]
	AllowedRetries int
}

func New[In, Out any](conf Conf[In, Out]) *Handler[In, Out] {
	return &Handler[In, Out]{
		processor:         conf.Worker,
		groupId:           conf.GroupId,
		bufferSize:        conf.BufferSize,
		destTopic:         conf.DestTopic,
		producerConf:      conf.ProducerConf,
		addrs:             conf.Addrs,
		sinks:             producer.NewPool(conf.AllowedRetries, conf.Addrs, conf.ProducerConf),
		allowedRetries:    conf.AllowedRetries,
		maxProcessingTime: conf.ProcessingTime,
	}
}

func (h *Handler[_, _]) Setup(session sarama.ConsumerGroupSession) error {
	return h.sinks.Init(session.Claims())
}

func (h *Handler[_, _]) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler[_, _]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	state := NewConsumeState(h.bufferSize, session, claim)

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Println("message channel was closed")
				return nil
			}
			state.currMessage = message
			err := h.processMessage(state)
			if err != nil {
				return err
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *Handler[_, _]) processMessage(state *consumeState) error {
	if len(state.buffer) == 0 {
		state.firstMessageBuffer = state.currMessage
	}

	produceMsg, err := h.processor.Run(h.destTopic, state.currMessage)
	if err != nil {
		log.Println(err)
		return nil
	}

	state.buffer = append(state.buffer, produceMsg)

	if state.shouldSendBuffer(h.bufferSize, h.maxProcessingTime) {
		if err := h.flushBuffer(state); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler[_, _]) flushBuffer(state *consumeState) error {
	offsets := getNextOffset(state.currMessage)

	if err := h.runSink(state, offsets); err != nil {
		return err
	}

	state.reset()
	return nil
}

func getNextOffset(message *sarama.ConsumerMessage) map[string][]*sarama.PartitionOffsetMetadata {
	return map[string][]*sarama.PartitionOffsetMetadata{
		message.Topic: {
			{
				Partition: message.Partition,
				Offset:    message.Offset + 1,
			},
		},
	}
}

func (h *Handler[_, _]) runSink(state *consumeState, offsets map[string][]*sarama.PartitionOffsetMetadata) error {
	err := h.sinks.Send(state.currMessage.Topic, state.currMessage.Partition, state.buffer, h.groupId, offsets)
	if err != nil {
		switch err {
		case producer.BadMessagesError: // processing was off try again.
			state.badProcessingCount++
			if state.badProcessingCount < h.allowedRetries {
				state.session.ResetOffset(state.firstMessageBuffer.Topic, state.firstMessageBuffer.Partition, state.firstMessageBuffer.Offset, "")
			} else {
				return err
			}
		case producer.BadProducerError:
			log.Printf("producer error that wasn't handled: %v", err)
			return err
		default:
			return err
		}
	}
	return nil
}
