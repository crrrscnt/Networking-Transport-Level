package kafka

import (
	"encoding/json"
	"fmt"
	"transport-layer-earth/internal/consts"
	"transport-layer-earth/internal/storage"
	"transport-layer-earth/internal/utils"

	"github.com/IBM/sarama"
)

func ReadFromKafka() error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create consumer
	consumer, err := sarama.NewConsumer([]string{consts.KafkaAddr}, config)
	if err != nil {
		return fmt.Errorf("error creating consumer: %w", err)
	}
	defer consumer.Close()

	// Connect consumer to topic
	partitionConsumer, err := consumer.ConsumePartition(consts.KafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("error opening topic: %w", err)
	}
	defer partitionConsumer.Close()

	// Infinite loop to read from Kafka and send to data link layer
	for {
		select {
		case message := <-partitionConsumer.Messages():
			segment := utils.Segment{}
			if err := json.Unmarshal(message.Value, &segment); err != nil {
				fmt.Printf("Error reading from Kafka: %v\n", err)
				continue
			}
			fmt.Printf(">From Kafka: sending segment to C-Layer:> %+v\n", segment)
			// fmt.Pt
			storage.AddPending(segment) // регистрируем сегмент как ожидающий ACK
			// fmt.Print(segment)
			go utils.SendSegment(segment) // Send to data link layer in a goroutine
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %s\n", err.Error())
		}
	}
}

func WriteToKafka(segment utils.Segment) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{consts.KafkaAddr}, config)
	if err != nil {
		return fmt.Errorf("error creating producer: %w", err)
	}
	defer producer.Close()

	// Convert segment to Kafka message
	segmentString, _ := json.Marshal(segment)
	message := &sarama.ProducerMessage{
		Topic: consts.KafkaTopic,
		Value: sarama.StringEncoder(segmentString),
	}

	// Send message
	_, _, err = producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}
