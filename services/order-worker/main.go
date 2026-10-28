package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	mainQueue      = "flashsale.orders"
	retryQueue     = "flashsale.orders.retry"
	retryExchange  = "flashsale.retry"
	dlxExchange    = "flashsale.dlx"
	maxRetries     = 3
	publishTimeout = 3 * time.Second
)

var uuidPattern = regexp.MustCompile(
	`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

type OrderMessage struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

// PermanentError represents a message that should not be retried.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func permanentError(message string) error {
	return &PermanentError{Err: errors.New(message)}
}

func main() {
	rabbitUser := envOr("RABBITMQ_USER", "flashsale")
	rabbitPassword := os.Getenv("RABBITMQ_PASSWORD")

	if rabbitPassword == "" {
		log.Fatal("RABBITMQ_PASSWORD is required")
	}

	rabbitURL := (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(rabbitUser, rabbitPassword),
		Host:   envOr("RABBITMQ_HOST", "localhost:5672"),
		Path:   "/",
	}).String()

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Open RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Qos(10, 0, false); err != nil {
		log.Fatalf("Set consumer prefetch: %v", err)
	}

	// Enable publisher confirms before publishing retries or DLQ messages.
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("Enable publisher confirms: %v", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	db, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Fatalf("Connect to PostgreSQL: %v", err)
	}
	defer db.Close(context.Background())

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Ping PostgreSQL: %v", err)
	}

	messages, err := ch.Consume(
		mainQueue,
		"flashsale-order-worker",
		false, // Manual acknowledgements.
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Start RabbitMQ consumer: %v", err)
	}

	log.Printf(
		"Order worker started; waiting for messages (max retries: %d)",
		maxRetries,
	)

	for {
		select {
		case <-ctx.Done():
			log.Println("Order worker shutting down")
			return

		case delivery, ok := <-messages:
			if !ok {
				log.Println("RabbitMQ delivery channel closed")
				return
			}

			err := processOrder(ctx, db, delivery.Body)
			if err == nil {
				if ackErr := delivery.Ack(false); ackErr != nil {
					log.Printf("Acknowledge processed order: %v", ackErr)
				}
				continue
			}

			log.Printf("Order processing failed: %v", err)

			retries := retryCount(delivery.Headers)

			var permanentErr *PermanentError
			if errors.As(err, &permanentErr) {
				log.Printf(
					"Permanent order error; sending directly to DLQ: %v",
					err,
				)
				retries = maxRetries
			}

			if retries >= maxRetries {
				log.Printf("Sending message to dead-letter queue")

				err = publishAndConfirm(
					ch,
					confirms,
					dlxExchange,
					mainQueue,
					delivery.Body,
					delivery.Headers,
				)
			} else {
				headers := copyHeaders(delivery.Headers)
				headers["x-retry-count"] = int32(retries + 1)

				log.Printf(
					"Scheduling retry %d of %d after 5 seconds",
					retries+1,
					maxRetries,
				)

				err = publishAndConfirm(
					ch,
					confirms,
					retryExchange,
					"retry",
					delivery.Body,
					headers,
				)
			}

			if err != nil {
				// Leave the original unacknowledged. Closing the channel
				// allows RabbitMQ to redeliver it.
				log.Printf(
					"Replacement publish failed; stopping worker: %v",
					err,
				)
				return
			}

			// Acknowledge the original only after the replacement is confirmed.
			if err := delivery.Ack(false); err != nil {
				log.Printf("Acknowledge original delivery: %v", err)
			}
		}
	}
}

type messagePublisher interface {
	Publish(
		exchange string,
		key string,
		mandatory bool,
		immediate bool,
		msg amqp.Publishing,
	) error
}

func publishAndConfirm(
	ch messagePublisher,
	confirms <-chan amqp.Confirmation,
	exchange string,
	routingKey string,
	body []byte,
	headers amqp.Table,
) error {
	err := ch.Publish(
		exchange,
		routingKey,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now().UTC(),
			Headers:      headers,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish replacement message: %w", err)
	}

	timer := time.NewTimer(publishTimeout)
	defer timer.Stop()

	select {
	case confirmation, ok := <-confirms:
		if !ok {
			return errors.New("publisher confirmation channel closed")
		}

		if !confirmation.Ack {
			return errors.New(
				"RabbitMQ negatively acknowledged replacement message",
			)
		}

		return nil

	case <-timer.C:
		return errors.New("timed out waiting for publisher confirmation")
	}
}

type orderDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

func processOrder(
	ctx context.Context,
	db orderDB,
	body []byte,
) error {
	var order OrderMessage

	if err := json.Unmarshal(body, &order); err != nil {
		return &PermanentError{
			Err: fmt.Errorf("invalid order JSON: %w", err),
		}
	}

	if !uuidPattern.MatchString(order.OrderID) {
		return permanentError("order_id must be a valid UUID")
	}

	if order.UserID == "" ||
		order.ProductID == "" ||
		order.Quantity != 1 ||
		order.CreatedAt.IsZero() {
		return permanentError("order message has missing or invalid fields")
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin order transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert only if this order ID is new.
	// RETURNING lets us avoid decrementing stock for duplicate deliveries.
	var insertedOrderID string
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (
			order_id, user_id, product_id, quantity, status, created_at
		)
		VALUES ($1, $2, $3, $4, 'accepted', $5)
		ON CONFLICT (order_id) DO NOTHING
		RETURNING order_id
	`,
		order.OrderID,
		order.UserID,
		order.ProductID,
		order.Quantity,
		order.CreatedAt,
	).Scan(&insertedOrderID)

	if errors.Is(err, pgx.ErrNoRows) {
		// This ID already exists. Verify the original order matches.
		var matches bool
		err = tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM orders
				WHERE order_id = $1
				  AND user_id = $2
				  AND product_id = $3
				  AND quantity = $4
			)
		`,
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
		).Scan(&matches)
		if err != nil {
			return fmt.Errorf("verify duplicate order: %w", err)
		}
		if !matches {
			return permanentError(
				"order ID already exists with conflicting order details",
			)
		}

		// The original order was already inserted. Do not deduct stock again.
		return tx.Commit(ctx)
	}
	if err != nil {
		return fmt.Errorf("insert order into PostgreSQL: %w", err)
	}

	// This order is new: reserve durable inventory inside the same transaction.
	result, err := tx.Exec(ctx, `
		UPDATE inventory
		SET stock = stock - $2,
		    updated_at = NOW()
		WHERE product_id = $1
		  AND stock >= $2
	`,
		order.ProductID,
		order.Quantity,
	)
	if err != nil {
		return fmt.Errorf("decrement PostgreSQL inventory: %w", err)
	}
	if result.RowsAffected() != 1 {
		return permanentError("insufficient PostgreSQL inventory")
	}

	result, err = tx.Exec(ctx, `
		UPDATE orders
		SET status = 'completed',
		    processed_at = NOW()
		WHERE order_id = $1
		  AND user_id = $2
		  AND product_id = $3
		  AND quantity = $4
	`,
		order.OrderID,
		order.UserID,
		order.ProductID,
		order.Quantity,
	)
	if err != nil {
		return fmt.Errorf("complete order in PostgreSQL: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("order completion affected an unexpected number of rows")
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit order transaction: %w", err)
	}

	log.Printf(
		"Completed order %s for user %s",
		order.OrderID,
		order.UserID,
	)

	return nil
}

func retryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}

	switch value := headers["x-retry-count"].(type) {
	case int8:
		return int(value)
	case int16:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case int:
		return value
	default:
		return 0
	}
}

func copyHeaders(original amqp.Table) amqp.Table {
	copied := make(amqp.Table)

	for key, value := range original {
		copied[key] = value
	}

	return copied
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
