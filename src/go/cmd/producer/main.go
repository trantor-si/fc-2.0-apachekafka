package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
) // Import the kafka package

var producer *kafka.Producer = NewKafkaProducer()

// Create a new Kafka producer
func main() {
	deliveryChan := make(chan kafka.Event)

	for i := 0; i < 20000; i++ {
		msgsent := fmt.Sprintf("Hello World #%d", i)

		// Para ir para o mesma partição
		// Publish(msgsent, "teste", producer, []byte("transferred"), deliveryChan)

		// Para ir para uma partição aleatória
		Publish(msgsent, "teste", producer, nil, deliveryChan)

		go DeliveryReport(deliveryChan)
	}

	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	// Mapa de configurações do Kafka para passar para o producer
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "go-kafka-1:9092",
		"delivery.timeout.ms": "100", // gera alguns erros por timeout
		"acks":                "all",
		"enable.idempotence":  "true",
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				result_msg := fmt.Sprintf("[ERROR] Delivery failed!!!! [%s][Topic: %s][Patition: %d][Offset: %v]",
					ev.Value, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				log.Printf("%s\n", result_msg)

				Publish(result_msg, "error", producer, []byte("process-error"), nil)
			} else {
				result_msg := fmt.Sprintf("Delivered message [%s][Topic: %s][Patition: %d][Offset: %v]",
					ev.Value, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				log.Printf("%s\n", result_msg)

				Publish(result_msg, "result", producer, []byte("process-result"), nil)
				// Exemplo: anotar no banco de dados que a mensagem foi enviada
			}
		}
	}
}
