package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "multi-event",
		Balancer: &kafka.LeastBytes{},
	})

	defer writer.Close()

	// Create 2 events
	userSignup := Event{
		Type: "user-signup",
		Data: map[string]interface{}{
			"id":    124,
			"email": "kafka@dev.com",
		},
	}

	orderCreated := Event{
		Type: "order-created",
		Data: map[string]interface{}{
			"order_id": 1,
			"amount":   1500,
		},
	}

	events := []Event{userSignup, orderCreated}

	for _, event := range events {
		jsonBytes, err := json.Marshal(event)
		if err != nil {
			log.Fatal("Failed to marshal event:", err)
		}

		msg := kafka.Message{
			Key:   []byte(event.Type),
			Value: jsonBytes,
		}

		err = writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatal("Failed to write message:", err)
		}

		fmt.Printf("Event %s sent to Kafka\n", event.Type)
	}

	fmt.Println("All events sent to Kafka!")
}
