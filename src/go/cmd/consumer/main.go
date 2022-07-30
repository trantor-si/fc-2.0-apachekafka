package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	consumer := NewKafkaConsumer([]string{"teste"})

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}

func NewKafkaConsumer(topics []string) *kafka.Consumer {
	// Mapa de configurações do Kafka para passar para o producer
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go-kafka-1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	c.SubscribeTopics(topics, nil)

	return c
}
