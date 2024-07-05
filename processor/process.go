package processor

import (
	"github.com/IBM/sarama"
	"io"
)

type Decoder[T any] interface {
	Decode(reader io.Reader) (T, error)
}

type Processor[In, Out any] interface {
	Process(In) (Out, error)
}

type Encoder[T any] interface {
	Encode(T) ([]byte, error)
}

type Flow struct {
	messageBus MessageBus
}

func flow(producerMessageBus MessageBus, consumerChannel chan []*sarama.ConsumerMessage) {

}
