package processor

import (
	"bytes"
	"context"
	"github.com/IBM/sarama"
	"log"
	"sync"
)

type Worker[T, S any] struct {
	output MessageBus

	input        chan []*sarama.ConsumerMessage
	errorHandler func([]*sarama.ConsumerMessage)

	processor Processor[T, S]

	decoder Decoder[T]
	encoder Encoder[S]
}

func (w *Worker[T, S]) run(wg *sync.WaitGroup, destTopic string, ctx context.Context) error {
	defer wg.Done()

	for {
		select {
		case messages, ok := <-w.input:
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			output := make([]*sarama.ProducerMessage, 0, len(messages))

			for _, msg := range messages {
				in, _ := w.decoder.Decode(bytes.NewReader(msg.Value))
				middle, _ := w.processor.Process(in)
				out, _ := w.encoder.Encode(middle)

				output = append(output, &sarama.ProducerMessage{
					Topic: destTopic,
					Key:   sarama.ByteEncoder(msg.Key),
					Value: sarama.ByteEncoder(out),
				})
			}

			w.output.Send(output[0].Partition, output[0].Topic, output)

		case <-ctx.Done():
			return nil
		}
	}
}
