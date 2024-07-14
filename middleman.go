package main

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/handler"
	"github.com/aryehlev/kafka-middleman/processor"
	"github.com/aryehlev/kafka-middleman/serde"
	"golang.org/x/sync/errgroup"
)

type MiddleMan[In, Out any] struct {
	handler   *handler.Handler[In, Out]
	topic     string
	consumers []sarama.ConsumerGroup
}

type Config[In, Out any] struct {
	NumConsumers int
	Addr         []string
	GroupId      string
	BufferSize   int
	SourceTopic  string
	DestTopic    string
	Process      processor.ProcessorFunc[In, Out]
	Decoder      processor.Decoder[In]
	Encoder      processor.Encoder[Out]
}

func New[In, Out any](groupId, sourceTopic, destTopic string, addr []string,
	processingFunc processor.ProcessorFunc[In, Out]) (*MiddleMan[In, Out], error) {
	return NewFromConfig(Config[In, Out]{
		NumConsumers: 1,
		Addr:         addr,
		GroupId:      groupId,
		BufferSize:   10000,
		SourceTopic:  sourceTopic,
		DestTopic:    destTopic,
		Process:      processingFunc,
		Decoder:      serde.JsonParser[In]{},
		Encoder:      serde.JsonEncoder[Out]{},
	})
}

func NewFromConfig[T, S any](conf Config[T, S]) (*MiddleMan[T, S], error) {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.IsolationLevel = sarama.ReadCommitted
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = false
	producerConfig := sarama.NewConfig()

	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Idempotent = true

	consumers := make([]sarama.ConsumerGroup, 0, conf.NumConsumers)
	for i := 0; i < conf.NumConsumers; i++ {
		consumer, err := sarama.NewConsumerGroup(conf.Addr, conf.GroupId, consumerConfig)
		if err != nil {
			return nil, err
		}
		consumers = append(consumers, consumer)
	}
	handle := handler.New(handler.Conf[T, S]{
		GroupId:      conf.GroupId,
		BufferSize:   conf.BufferSize,
		DestTopic:    conf.DestTopic,
		ProducerConf: *producerConfig,
		Addrs:        conf.Addr,
		Worker:       processor.New(conf.Process, conf.Decoder, conf.Encoder),
	})

	return &MiddleMan[T, S]{
		handler:   handle,
		topic:     conf.SourceTopic,
		consumers: consumers,
	}, nil
}

func (m MiddleMan[T, S]) Run(ctx context.Context) error {
	errGroup, _ := errgroup.WithContext(ctx)

	for _, consumer := range m.consumers {
		consumer := consumer
		errGroup.Go(func() error {
			for {
				if err := consumer.Consume(ctx, []string{m.topic}, m.handler); err != nil {
					if errors.Is(err, sarama.ErrClosedConsumerGroup) {
						return nil
					}
					return err
				}

				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
		})
	}

	return errGroup.Wait()
}
