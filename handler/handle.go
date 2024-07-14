package handler

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/processor"
	"github.com/aryehlev/kafka-middleman/producer"
)

type Handler[T, S any] struct {
	processor          processor.Worker[T, S]
	sinks              map[int32]*producer.Worker
	topic              string
	groupId            string
	buffer             []*sarama.ProducerMessage
	firstMessageBuffer *sarama.ConsumerMessage
	bufferSize         int
	destTopic          string
	producerConf       *sarama.Config
	addrs              []string

	allowedBadProcessingCount int
	badProcessingCount        int
}

type Conf[T, S any] struct {
	GroupId      string
	BufferSize   int
	DestTopic    string
	ProducerConf *sarama.Config
	Addrs        []string
	Worker       processor.Worker[T, S]
}

func New[T, S any](conf Conf[T, S]) *Handler[T, S] {
	return &Handler[T, S]{
		processor:    conf.Worker,
		groupId:      conf.GroupId,
		buffer:       make([]*sarama.ProducerMessage, 0, conf.BufferSize),
		bufferSize:   conf.BufferSize,
		destTopic:    conf.DestTopic,
		producerConf: conf.ProducerConf,
		addrs:        conf.Addrs,
		sinks:        make(map[int32]*producer.Worker),
	}
}

func (h *Handler[_, _]) Setup(session sarama.ConsumerGroupSession) error {
	for _, partition := range session.Claims()[h.topic] {
		sink, err := producer.New(h.destTopic, h.producerConf, h.addrs)
		if err != nil {
			log.Printf("failed to create producer: %v", err)
			return err
		}
		h.sinks[partition] = sink
	}
	return nil
}

func (h *Handler[_, _]) Cleanup(session sarama.ConsumerGroupSession) error {
	for _, worker := range h.sinks {
		worker.Close()
	}
	return nil
}

func (h *Handler[_, _]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Println("message channel was closed")
				return nil
			}

			err := h.processMessage(session, message, claim.Partition())
			if err != nil {
				return err
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *Handler[_, _]) processMessage(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, partition int32) error {
	if len(h.buffer) == 0 {
		h.firstMessageBuffer = message
	}

	produceMsg, err := h.processor.Run(message)
	if err != nil {
		session.MarkOffset(message.Topic, message.Partition, message.Offset+1, "")
		return nil
	}

	h.buffer = append(h.buffer, produceMsg)
	if len(h.buffer) >= h.bufferSize {
		if err := h.flushBuffer(session, partition, message); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler[_, _]) flushBuffer(session sarama.ConsumerGroupSession, partition int32, message *sarama.ConsumerMessage) error {
	offsets := map[string][]*sarama.PartitionOffsetMetadata{
		message.Topic: {
			{
				Partition: message.Partition,
				Offset:    message.Offset + 1,
			},
		},
	}

	sink := h.sinks[partition]
	if err := h.runSink(sink, session, message, offsets); err != nil {
		return err
	}

	h.badProcessingCount = 0
	h.buffer = h.buffer[:0]

	return nil
}

func (h *Handler[_, _]) runSink(sink *producer.Worker, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, offsets map[string][]*sarama.PartitionOffsetMetadata) error {
	err := sink.Run(h.buffer, h.groupId, offsets)
	if err != nil {
		switch err {
		case producer.BadMessagesError: // processing was off try again.
			h.badProcessingCount++
			if h.badProcessingCount < h.allowedBadProcessingCount {
				session.ResetOffset(h.firstMessageBuffer.Topic, h.firstMessageBuffer.Partition, h.firstMessageBuffer.Offset, "")
			} else {
				return err
			}
		case producer.BadProducerError:
			log.Printf("producer error, attempting restart: %v", err)
			if restartErr := sink.Restart(); restartErr != nil {
				return restartErr
			}
			if retryErr := sink.Run(h.buffer, h.groupId, nil); retryErr != nil {
				return retryErr
			}
		default:
			return err
		}
	}
	return nil
}
