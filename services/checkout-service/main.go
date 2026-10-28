package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"flashsale/checkout-service/handlers"
	"flashsale/checkout-service/inventory"
	"flashsale/checkout-service/queue"

	"github.com/redis/go-redis/v9"
)

func main() {
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisPassword == "" {
		log.Fatal("REDIS_PASSWORD environment variable is required")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         envOr("REDIS_ADDR", "localhost:6379"),
		Password:     redisPassword,
		DB:           0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	rabbitUser := envOr("RABBITMQ_USER", "guest")
	rabbitPassword := os.Getenv("RABBITMQ_PASSWORD")

	if rabbitPassword == "" {
		log.Fatal("RABBITMQ_PASSWORD environment variable is required")
	}

	rabbitURL := "amqp://" + rabbitUser + ":" + rabbitPassword + "@localhost:5672/"

	publisher, err := queue.NewPublisher(rabbitURL)
	if err != nil {
		log.Fatalf("RabbitMQ initialization failed: %v", err)
	}
	defer publisher.Close()

	log.Println("RabbitMQ publisher connected")

	reserver := inventory.NewReserver(rdb)
	checkoutHandler := handlers.NewCheckoutHandler(reserver, publisher)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", checkoutHandler.Health)
	mux.HandleFunc("POST /checkout", checkoutHandler.Checkout)

	addr := envOr("HTTP_ADDR", ":8080")
	log.Printf("Checkout API listening on %s", addr)

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Fatal(server.ListenAndServe())
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
