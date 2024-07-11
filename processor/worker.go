package processor

import (
	"github.com/IBM/sarama"
)

type Worker[T, S any] struct {
	processor Processor[T, S]
	decoder   Decoder[T]
	encoder   Encoder[S]
}

func New[T, S any](processor Processor[T, S],
	decoder Decoder[T],
	encoder Encoder[S]) *Worker[T, S] {
	return &Worker[T, S]{
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
	middle, err := w.processor.Process(in)
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
