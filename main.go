package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/aryehlev/kafka-middleman/handler"
	"github.com/aryehlev/kafka-middleman/models"
)

const (
	groupIdEnv   = "GROUP_ID"
	addressesEnv = "ADRESSES"
	topicEnv     = "TOPIC"
	loopTimeEnv  = "LOOP_TIME"
)

func main() {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.IsolationLevel = sarama.ReadCommitted
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = false
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	producerConfig := sarama.NewConfig()

	producerConfig.Net.MaxOpenRequests = 1
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Idempotent = true

	addresses := strings.Split(os.Getenv(addressesEnv), ",")
	if len(addresses) < 1 {
		log.Fatalf("please provide valid addresses %s", addressesEnv)
	}

	groupId := os.Getenv(groupIdEnv)
	if len(groupId) < 1 {
		log.Fatalf("please provide valid group id %s", groupIdEnv)
	}
	consumerGroup, err := sarama.NewConsumerGroup(addresses, groupId, consumerConfig)
	if err != nil {
		log.Fatalf("Error creating handler group client: %v", err)
	}
	topic := os.Getenv(topicEnv)
	if len(groupId) < 1 {
		log.Fatalf("please provide valid group id %s", groupIdEnv)
	}

	timeoutforLoop := os.Getenv(loopTimeEnv)
	timeForLoop, err := strconv.Atoi(timeoutforLoop)
	if err != nil || timeForLoop == 0 {
		timeForLoop = 10
	}
	loopInSeconds := time.Second * time.Duration(timeForLoop)

	ctx, cancel := context.WithTimeout(context.Background(), loopInSeconds)
	defer cancel()

	var h handler.Handler[models.Message, *models.Message]
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, strings.Split(topic, ","), &h); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from handler: %v", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	wg.Wait()
}
