package processor

import (
	"github.com/IBM/sarama"
)

type ProcessorFunc[In, Out any] func(In) (Out, error)

type Worker[In, Out any] struct {
	processor ProcessorFunc[In, Out]
	decoder   Decoder[In]
	encoder   Encoder[Out]
}

func New[In, Out any](processor ProcessorFunc[In, Out],
	decoder Decoder[In],
	encoder Encoder[Out]) Worker[In, Out] {
	return Worker[In, Out]{
		processor: processor,
		decoder:   decoder,
		encoder:   encoder,
	}
}
func (w *Worker[T, S]) Run(msg *sarama.ConsumerMessage) (*sarama.ProducerMessage, error) {
	in, err := w.decoder.Decode(msg.Value)
	if err != nil {
		return nil, err
	}
	middle, err := w.processor(in)
	if err != nil {
		return nil, err
	}
	out, err := w.encoder.Encode(middle)
	if err != nil {
		return nil, err
	}

	msg.Value = out

	return &sarama.ProducerMessage{
		Topic: msg.Topic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(out),
	}, nil
}
