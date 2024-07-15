package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/models"
	"github.com/aryehlev/kafka-middleman/processor"
	"github.com/bitfield/script"
)

const (
	testInputTopic = "inputTestTopic"
	testOutTopic   = "outputTestTopic"
	testGroup      = "testGroupMiddleMan"

	numOfIterations = 10000
)

var addr = []string{":9094", ":9093", ":9092"}

func TestKafkaProcessing(t *testing.T) {
	setupDocker(t)
	defer func() {
		_, err := script.Exec("docker compose kill").Stdout()
		if err != nil {
			t.Error(err)
			return
		}
	}()
	sarama.Logger = log.New(os.Stdout, "test:", log.LstdFlags)

	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(addr, config)
	if err != nil {
		log.Fatalf("Error creating admin client: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Fatalf("Error closing admin client: %v", err)
		}
	}()

	err = createTopic(admin, testInputTopic, 10, 1)
	if err != nil {
		log.Fatal(err)
	}

	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	messages := []models.Message{
		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().AddDate(0, 0, -3).Format(time.RFC3339), Data: "OldData1"},
		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().AddDate(0, 0, -4).Format(time.RFC3339), Data: "OldData2"},
		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.RFC3339), Data: "This is a long data string"},
		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.RFC3339), Data: "Another long data string"},
		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.RFC3339), Data: "Short1"},
		{ID: fmt.Sprintf("%d", rand.Int()), Timestamp: time.Now().Format(time.RFC3339), Data: "Short2"},
	}

	produceMessages(t, producer, messages)
	runProcessor(t)
	validateOutput(t, config)
}

func setupDocker(t *testing.T) {
	go func() {
		_, err := script.Exec("docker-compose up --build").Stdout()
		if err != nil {
			t.Error(err)
			return
		}
	}()

	time.Sleep(time.Minute)
}

func createTopic(admin sarama.ClusterAdmin, topic string, numPartitions int32, replicationFactor int16) error {
	topicDetail := sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	err := admin.CreateTopic(topic, &topicDetail, false)
	if err != nil {
		return fmt.Errorf("error creating topic: %v", err)
	}
	fmt.Println("Topic created successfully")
	return nil
}

func produceMessages(t *testing.T, producer sarama.SyncProducer, messages []models.Message) {
	for i := 0; i < numOfIterations; i++ {
		for _, message := range messages {
			msgValue := fmt.Sprintf(`{"id":"%s","timestamp":"%s","data":"%s"}`, message.ID, message.Timestamp, message.Data)
			kafkaMessage := &sarama.ProducerMessage{
				Topic: testInputTopic,
				Value: sarama.StringEncoder(msgValue),
			}

			partition, offset, err := producer.SendMessage(kafkaMessage)
			if err != nil {
				t.Fatalf("Failed to send message: %v", err)
			} else {
				t.Logf("Message sent to partition %d at offset %d\n", partition, offset)
			}
		}
	}
}

func runProcessor(t *testing.T) {
	pro := processor.Validator{
		SupportedTimeFormats: []string{time.RFC3339},
	}
	mid, err := New(testGroup, testInputTopic, testOutTopic, addr, pro.Process)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := mid.Run(context.Background()); err != nil {
			t.Error(err)
			return
		}
	}()

	time.Sleep(2 * time.Minute)
}

func validateOutput(t *testing.T, config *sarama.Config) {
	consumer, err := sarama.NewConsumer(addr, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(testOutTopic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("Failed to start consumer for partition: %v", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	timeout := time.After(3 * time.Minute)
	numMessages := 0
	for {
		select {
		case _ = <-partitionConsumer.Messages():
			numMessages++
		case <-timeout:
			fmt.Printf("num of messages %d", numMessages)
			if numMessages < numOfIterations*4/8-300 { // num of ok messages (2 not ok) devided by partitions with buffer
				t.Errorf("got number messages %d, expected: %d", numMessages, numOfIterations*4)
			}
			return
		}
	}
}
