package producer

import (
	"errors"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

const transactionIdFormat = "midman_%s_%d"

var BadMessagesError = errors.New("bad Messages error")
var BadProducerError = errors.New("bad producer error")
var UnknownError = errors.New("unknown error")

type Producer struct {
	destTopic string
	producer  sarama.SyncProducer

	addrs []string

	conf *sarama.Config

	numOfRestartTries int
}

func New(destTopic string, conf sarama.Config, addrs []string, srcTopic string, partition int32) (*Producer, error) {
	conf.Producer.Transaction.ID = fmt.Sprintf(transactionIdFormat, srcTopic, partition)
	producer, err := sarama.NewSyncProducer(addrs, &conf)
	if err != nil {
		return nil, err
	}
	return &Producer{
		destTopic:         destTopic,
		producer:          producer,
		addrs:             addrs,
		conf:              &conf,
		numOfRestartTries: 3,
	}, nil
}

func (w *Producer) run(messages []*sarama.ProducerMessage, groupId string, offsets map[string][]*sarama.PartitionOffsetMetadata) error {
	err := w.producer.BeginTxn()
	if err != nil {
		log.Println(err)
		return w.Error(err)
	}
	err = w.producer.SendMessages(messages)
	if err != nil {
		log.Println(err)
		return w.Error(err)
	}

	err = w.producer.AddOffsetsToTxn(offsets, groupId)
	if err != nil {
		log.Println(err)
		return w.Error(err)
	}

	err = w.producer.CommitTxn()
	if err != nil {
		log.Println(err)
		return w.Error(err)
	}

	return nil
}

func (w *Producer) Close() error {
	return w.producer.Close()
}

func (w *Producer) Error(err error) error {
	if w.producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
		return BadProducerError
	}
	if w.producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
		err := w.producer.AbortTxn()
		if err != nil {
			return BadProducerError
		}

		return BadMessagesError
	}

	if errProducer, ok := err.(sarama.ProducerErrors); ok { // TODO check for nil and deal with all errors.
		for _, pe := range errProducer {
			switch pe.Err {
			case sarama.ErrOutOfBrokers:
				return BadProducerError
			case sarama.ErrMessageSizeTooLarge, sarama.ErrInvalidMessage:
				return BadMessagesError
			}
		}
	}

	return UnknownError
}
