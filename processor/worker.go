package processor

import (
	"github.com/IBM/sarama"
)

type Worker[T, S any] struct {
	processor Processor[T, S]
	decoder   Decoder[T]
	encoder   Encoder[S]
}

func (w *Worker[T, S]) Run(msg *sarama.ConsumerMessage) error {
	in, err := w.decoder.Decode(msg.Value)
	if err != nil {
		return err
	}
	middle, err := w.processor.Process(in)
	if err != nil {
		return err
	}
	out, err := w.encoder.Encode(middle)
	if err != nil {
		return err
	}

	msg.Value = out

	return nil
}
