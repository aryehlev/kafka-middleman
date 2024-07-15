package main

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/processor"
	"log"
	"time"
)

const (
	groupIdEnv   = "GROUP_ID"
	addressesEnv = "ADRESSES"
	srcTpicEnv   = "TOPIC"
	destTopicEnv = "TOPIC"
	loopTimeEnv  = "LOOP_TIME"
)

func main() {
	//config := sarama.NewConfig()
	//config.Producer.Return.Successes = true
	//producer, err := sarama.NewSyncProducer([]string{":9094", ":9093", ":9092"}, config)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer func() {
	//	if err := producer.Close(); err != nil {
	//		panic(err)
	//	}
	//}()
	//
	//for i := 0; i < 1000; i++ {
	//	messages := []models.Message{
	//		// 2 messages with date more than 2 days old
	//		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().AddDate(0, 0, -3).Format(time.DateOnly), Data: "OldData1", Status: "old"},
	//		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().AddDate(0, 0, -4).Format(time.DateOnly), Data: "OldData2", Status: "old"},
	//		// 2 messages with Data length more than 10 bytes
	//		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.DateOnly), Data: "This is a long data string", Status: "long"},
	//		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.DateOnly), Data: "Another long data string", Status: "long"},
	//		// 2 messages with date today and Data length less than 10 bytes
	//		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.DateOnly), Data: "Short1", Status: "short"},
	//		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.DateOnly), Data: "Short2", Status: "short"},
	//	}
	//
	//	for _, message := range messages {
	//		// Serialize the message to JSON
	//		msgValue := fmt.Sprintf(`{"id":"%s","timestamp":"%s","data":"%s","status":"%s"}`, message.ID, message.Timestamp, message.Data, message.Status)
	//
	//		kafkaMessage := &sarama.ProducerMessage{
	//			Topic: "test1",
	//			Value: sarama.StringEncoder(msgValue),
	//		}
	//
	//		partition, offset, err := producer.SendMessage(kafkaMessage)
	//		if err != nil {
	//			fmt.Printf("Failed to send message: %v\n", err)
	//		} else {
	//			fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	//		}
	//	}
	//}

	//
	pro := processor.Validator{
		SupportedTimeFormats: []string{time.DateOnly},
	}
	mid, err := New("00100", "test1",
		"outTopicTest1", []string{":9094", ":9093", ":9092"}, pro.Process)
	if err != nil {
		log.Fatal(err)
	}
	err = mid.Run(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	//conf := sarama.NewConfig()
	//consumer, err := sarama.NewConsumer([]string{":9094", ":9093", ":9092"}, conf)
	//if err != nil {
	//	panic(err)
	//}
	//defer func() {
	//	if err := consumer.Close(); err != nil {
	//		panic(err)
	//	}
	//}()
	//
	//outTopic := "inputTopic"
	//partitionConsumer, err := consumer.ConsumePartition(outTopic, 0, sarama.OffsetOldest)
	//if err != nil {
	//	log.Fatalln("Failed to start consumer for partition", err)
	//}
	//defer func() {
	//	if err := partitionConsumer.Close(); err != nil {
	//		panic(err)
	//	}
	//}()
	//
	//// Read messages from Kafka topic
	//fmt.Println("Reading messages from outTopic:")
	//for message := range partitionConsumer.Messages() {
	//	fmt.Printf("Message received: %s\n", string(message.Value))
	//}

	//config := sarama.NewConfig()
	////config.Version = sarama.V2_6_0_0
	////config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//
	//consumerGroup, err := sarama.NewConsumerGroup([]string{":9094", ":9093", ":9092"}, "my-group000", config)
	//if err != nil {
	//	log.Fatal("Error creating consumer group: ", err)
	//}
	//producerConfig := sarama.NewConfig()
	//
	//producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	//producerConfig.Producer.Idempotent = true
	//producerConfig.Producer.Return.Successes = true
	//producerConfig.Producer.Return.Errors = true
	//producerConfig.Net.MaxOpenRequests = 1
	//handler1 := handler.New(handler.Conf[*models.Message, *models.Message]{
	//	GroupId:        "my-group000",
	//	BufferSize:     10,
	//	DestTopic:      "destTopic",
	//	ProducerConf:   *producerConfig,
	//	Addrs:          []string{":9094", ":9093", ":9092"},
	//	Worker:         processor.New[*models.Message, *models.Message](pro.Process, serde.JsonParser[*models.Message]{}, serde.JsonEncoder[*models.Message]{}),
	//	AllowedRetries: 3,
	//})
	//
	//for {
	//	if err := consumerGroup.Consume(context.Background(), []string{"inputTopic"}, handler1); err != nil {
	//		log.Fatal("Error consuming from group: ", err)
	//	}
	//}
}

type Handler struct{}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
