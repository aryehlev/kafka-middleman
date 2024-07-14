package producer

type topicPartition struct {
	topic     string
	partition int32
}

type Pool struct {
	workers map[topicPartition]*Worker
	New     func(topic string, partition int32) (*Worker, error)
}

func (p *Pool) Get(topic string, partition int32) *Worker {
	tp := topicPartition{topic: topic, partition: partition}
	return p.workers[tp]
}

// Input: map of topics to partitions.
func (p *Pool) Init(claims map[string][]int32) error {
	for topic, partitions := range claims {
		for _, partition := range partitions {
			if err := p.add(topic, partition); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Pool) Close(claims map[string][]int32) {
	for topic, partitions := range claims {
		for _, partition := range partitions {
			p.close(topic, partition)
		}
	}
}

func (p *Pool) add(topic string, partition int32) error {
	tp := topicPartition{topic: topic, partition: partition}
	var err error
	p.workers[tp], err = p.New(topic, partition)

	return err
}

func (p *Pool) close(topic string, partition int32) {
	tp := topicPartition{topic: topic, partition: partition}
	worker, ok := p.workers[tp]
	if ok {
		worker.Close()
	}
}
