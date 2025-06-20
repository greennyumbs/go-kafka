package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type UserSignupData struct {
	ID    int    `json:"id"`
	Email string `json:"email"`
}

type OrderCreatedData struct {
	OrderID int `json:"order_id"`
	Amount  int `json:"amount"`
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "multi-event",
		GroupID: "event-processor",
	})
	defer reader.Close()

	fmt.Println("Consumer started...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Error reading message:", err)
		}
		var event Event
		err = json.Unmarshal(m.Value, &event)
		if err != nil {
			log.Fatal("Failed to unmarshal event:", err)
			continue
		}

		switch event.Type {
		case "user-signup":
			var data UserSignupData
			err = json.Unmarshal(event.Data, &data)
			if err != nil {
				log.Fatal("Failed to unmarshal user signup data:", err)
				continue
			}
			fmt.Printf("User signup event received: ID=%d, Email=%s\n", data.ID, data.Email)

		case "order-created":
			var data OrderCreatedData
			err = json.Unmarshal(event.Data, &data)
			if err != nil {
				log.Fatal("Failed to unmarshal order created data:", err)
			}
			fmt.Printf("Order created event received: OrderID=%d, Amount=%d\n", data.OrderID, data.Amount)

		default:
			fmt.Printf("Unknown event type: %s\n", event.Type)
		}
	}
}
