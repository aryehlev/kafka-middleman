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

type MiddleMan[T, S any] struct {
	handler   *handler.Handler[T, S]
	topic     string
	consumers []sarama.ConsumerGroup
}

type Config[T, S any] struct {
	NumConsumers int
	Addr         []string
	GroupId      string
	BufferSize   int
	SourceTopic  string
	DestTopic    string
	Process      processor.Processor[T, S]
	Decoder      processor.Decoder[T]
	Encoder      processor.Encoder[S]
}

func New[T, S any](groupId, sourceTopic, destTopic string, addr []string,
	processingFunc func(T) (S, error)) (*MiddleMan[T, S], error) {
	return NewFromConfig(Config[T, S]{
		NumConsumers: 1,
		Addr:         addr,
		GroupId:      groupId,
		BufferSize:   10000,
		SourceTopic:  sourceTopic,
		DestTopic:    destTopic,
		Process:      processor.ProcessorFromFunc(processingFunc),
		Decoder:      serde.JsonParser[T]{},
		Encoder:      serde.JsonEncoder[S]{},
	})
}

func NewFromConfig[T, S any](conf Config[T, S]) (*MiddleMan[T, S], error) {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.IsolationLevel = sarama.ReadCommitted
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = false
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	producerConfig := sarama.NewConfig()

	producerConfig.Net.MaxOpenRequests = 5
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
		ProducerConf: producerConfig,
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
