package producer

import (
	"github.com/IBM/sarama"
	"log"
)

type Worker struct {
	destTopic string
	producer  sarama.AsyncProducer

	addrs []string

	conf *sarama.Config

	numOfRestartTries int
}

func (w *Worker) Run(message *sarama.ConsumerMessage, groupId string) (bool, bool) {
	err := w.producer.BeginTxn()
	if err != nil {
		log.Println(err)
		return true, true
	}
	w.producer.Input() <- &sarama.ProducerMessage{
		Topic: destinationTopic,
		Key:   sarama.ByteEncoder(message.Key),
		Value: sarama.ByteEncoder(message.Value),
	}

	err = w.producer.AddMessageToTxn(message, groupId, nil)
	if err != nil {
		log.Println(err)
		return w.shouldRestartAndReset()
	}

	err = w.producer.CommitTxn()
	if err != nil {
		log.Println(err)
		return w.shouldRestartAndReset()
	}

	return false, false
}

func (w *Worker) Close() error {
	return nil
}

func (w *Worker) Restart() error {
	var err error
	for i := 0; i < w.numOfRestartTries; i++ {
		err = w.restart()
		if err == nil {
			return nil
		}
	}

	return err
}

func (w *Worker) restart() error {
	producer, err := sarama.NewAsyncProducer(w.addrs, w.conf)
	if err != nil {
		log.Println(err)
		return nil
	}

	w.producer = producer

	return nil
}

func (w *Worker) shouldRestartAndReset() (bool, bool) {
	if w.producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
		return true, true
	}
	if w.producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
		err := w.producer.AbortTxn()
		if err != nil {
			return true, false
		}

		return false, true
	}

	return false, false
}
