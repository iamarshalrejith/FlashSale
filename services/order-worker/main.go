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
	"github.com/jackc/pgx/v5/pgconn"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	mainQueue      = "flashsale.orders"
	retryQueue     = "flashsale.orders.retry"
	retryExchange  = "flashsale.retry"
	dlxExchange    = "flashsale.dlx"
	maxRetries     = 3
	publishTimeout = 3 * time.Second

	// stuckOrderThreshold: how long an order may sit in 'accepted' before the
	// recovery loop re-queues it. 10 minutes comfortably exceeds normal
	// processing latency while catching genuinely lost messages.
	stuckOrderThreshold = 10 * time.Minute

	// recoveryInterval: how often the stuck-order scan runs.
	recoveryInterval = 5 * time.Minute
)

// releaseScript atomically restores a Redis inventory reservation.
// Mirrors checkout-service/inventory/release.lua so both services use the
// same semantics when releasing a slot.
const releaseScript = `
local inventory_key = KEYS[1]
local order_id      = ARGV[1]
 
if not order_id or order_id == "" then
    return {0, "INVALID_ORDER"}
end
 
local reservation_key = inventory_key .. ":reservation:" .. order_id
local quantity = tonumber(redis.call("GET", reservation_key) or "0")
 
if not quantity or quantity < 1 then
    return {0, "NO_RESERVATION"}
end
 
local remaining = redis.call("INCRBY", inventory_key, quantity)
redis.call("DEL", reservation_key)
 
return {1, tostring(remaining)}
`

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

	// Redis client — used to clean up reservation keys and to restore
	// inventory when an order is permanently failed.
	rdb := redis.NewClient(&redis.Options{
		Addr:     envOr("REDIS_ADDR", "localhost:6379"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Ping Redis: %v", err)
	}

	// On startup, delete orphaned reservation keys for orders that PostgreSQL
	// already considers completed or failed. These accumulate when the worker
	// commits a transaction but crashes before the DEL runs.
	if err := reconcileCompletedOrders(ctx, db, rdb); err != nil {
		log.Printf("Startup reconcile warning (non-fatal): %v", err)
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

	// Recovery goroutine uses its own DB and AMQP connections so it never
	// races with the main consumer loop.
	go recoverStuckOrders(ctx, dbURL, rabbitURL, rdb)

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
				// Order committed to PostgreSQL. The Redis inventory counter
				// was already decremented at checkout; we only remove the
				// per-order reservation marker the checkout service created.
				cleanupRedisReservation(ctx, rdb, delivery.Body)

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

				// Restore inventory only when the reservation can be identified
				// and safely released.
				if releaseErr := releaseRedisReservation(
					ctx,
					rdb,
					delivery.Body,
				); releaseErr != nil {
					log.Printf("Cannot safely dead-letter order; stopping worker: %v", releaseErr)
					return
				}

				// Record a terminal failure before dead-lettering.
				order, parseErr := parseOrderForFailure(delivery.Body)
				if parseErr != nil {
					log.Printf("Cannot record terminal order failure: %v", parseErr)
					return
				}

				if failErr := markOrderFailed(ctx, db, order); failErr != nil {
					log.Printf("Cannot record terminal order failure; stopping worker: %v", failErr)
					return
				}

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
				log.Printf(
					"Replacement publish failed; stopping worker: %v",
					err,
				)
				return
			}

			// Acknowledge only after the replacement is confirmed.
			if err := delivery.Ack(false); err != nil {
				log.Printf("Acknowledge original delivery: %v", err)
			}
		}
	}
}

// cleanupRedisReservation removes the per-order reservation marker after a
// successful PostgreSQL commit. The main inventory counter (inventory:<id>)
// was decremented at checkout and must not be changed here.
func cleanupRedisReservation(ctx context.Context, rdb *redis.Client, body []byte) {
	var order OrderMessage
	if err := json.Unmarshal(body, &order); err != nil || order.ProductID == "" || order.OrderID == "" {
		return
	}

	key := "inventory:" + order.ProductID + ":reservation:" + order.OrderID
	if err := rdb.Del(ctx, key).Err(); err != nil {
		log.Printf("Redis cleanup warning: could not delete %s: %v", key, err)
	}
}

// releaseRedisReservation atomically restores the Redis inventory counter
// for an order that will not be retried.
//
// Redis command errors are returned so the caller can avoid acknowledging
// the original RabbitMQ delivery when restoration fails.
func releaseRedisReservation(
	ctx context.Context,
	rdb *redis.Client,
	body []byte,
) error {
	var order OrderMessage

	if err := json.Unmarshal(body, &order); err != nil {
		// Invalid JSON cannot identify a reservation. The message can
		// still be sent to the DLQ.
		return nil
	}

	if order.ProductID == "" || order.OrderID == "" {
		// No usable reservation key is available.
		return nil
	}

	vals, err := rdb.Eval(
		ctx,
		releaseScript,
		[]string{"inventory:" + order.ProductID},
		order.OrderID,
	).Slice()
	if err != nil {
		return fmt.Errorf(
			"restore Redis reservation for order %s: %w",
			order.OrderID,
			err,
		)
	}

	if len(vals) != 2 {
		return fmt.Errorf(
			"unexpected Redis release response for order %s",
			order.OrderID,
		)
	}

	log.Printf(
		"Redis release for order %s: status=%v remaining=%v",
		order.OrderID,
		vals[0],
		vals[1],
	)

	return nil
}

// reconcileCompletedOrders removes orphaned Redis reservation keys for orders
// that PostgreSQL already considers completed or failed. Runs once on startup.
//
// Source of truth: PostgreSQL.
// Redis reservation keys are ephemeral metadata; their absence never causes
// incorrect stock counts because the main Redis counter was adjusted
// atomically at checkout and is restored when an order is released.
func reconcileCompletedOrders(ctx context.Context, db *pgx.Conn, rdb *redis.Client) error {
	rows, err := db.Query(ctx, `
		SELECT order_id, product_id
		FROM orders
		WHERE status IN ('completed', 'failed')
	`)
	if err != nil {
		return fmt.Errorf("query completed/failed orders: %w", err)
	}
	defer rows.Close()

	var cleaned int

	for rows.Next() {
		var orderID, productID string
		if err := rows.Scan(&orderID, &productID); err != nil {
			log.Printf("Reconcile scan: %v", err)
			continue
		}

		key := "inventory:" + productID + ":reservation:" + orderID
		n, err := rdb.Del(ctx, key).Result()
		if err != nil {
			log.Printf("Reconcile: could not delete %s: %v", key, err)
			continue
		}
		if n > 0 {
			cleaned++
			log.Printf("Reconcile: removed orphaned reservation key for order %s", orderID)
		}
	}

	if cleaned > 0 {
		log.Printf("Reconcile: cleaned %d orphaned reservation key(s)", cleaned)
	}

	return rows.Err()
}

// recoverStuckOrders runs on its own connections so it never races with the
// main consumer loop. Every recoveryInterval it looks for orders that have
// been in 'accepted' longer than stuckOrderThreshold and re-publishes them
// to the main queue. processOrder's idempotency logic makes this safe:
// - If the order already completed: the duplicate path returns early.
// - If the order never committed: it is processed for the first time.
func recoverStuckOrders(ctx context.Context, dbURL, rabbitURL string, rdb *redis.Client) {
	ticker := time.NewTicker(recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := doRecovery(ctx, dbURL, rabbitURL, rdb); err != nil {
				log.Printf("Stuck-order recovery error: %v", err)
			}
		}
	}
}

func doRecovery(
	ctx context.Context,
	dbURL string,
	rabbitURL string,
	rdb *redis.Client,
) error {
	db, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("recovery DB connect: %w", err)
	}
	defer db.Close(ctx)

	rabbitConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return fmt.Errorf("recovery AMQP connect: %w", err)
	}
	defer rabbitConn.Close()

	rabbitCh, err := rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("recovery AMQP channel: %w", err)
	}
	defer rabbitCh.Close()

	if err := rabbitCh.Confirm(false); err != nil {
		return fmt.Errorf("recovery enable confirms: %w", err)
	}

	recoveryConfirms := rabbitCh.NotifyPublish(
		make(chan amqp.Confirmation, 1),
	)

	// Load stuck orders using the independently testable query helper.
	orders, err := loadStuckOrders(ctx, db, stuckOrderThreshold)
	if err != nil {
		return err
	}

	var requeued int

	for _, order := range orders {
		message := OrderMessage{
			OrderID:   order.OrderID,
			UserID:    order.UserID,
			ProductID: order.ProductID,
			Quantity:  order.Quantity,
			CreatedAt: order.CreatedAt,
		}

		body, err := json.Marshal(message)
		if err != nil {
			log.Printf(
				"Recovery marshal for order %s: %v",
				order.OrderID,
				err,
			)
			continue
		}

		age := time.Since(order.CreatedAt).Round(time.Second)
		log.Printf(
			"Recovery: re-queuing stuck order %s (stuck for %s)",
			order.OrderID,
			age,
		)

		if err := publishAndConfirm(
			rabbitCh,
			recoveryConfirms,
			"",
			mainQueue,
			body,
			nil,
		); err != nil {
			log.Printf(
				"Recovery: re-queue failed for order %s: %v",
				order.OrderID,
				err,
			)
			continue
		}

		requeued++
	}

	if requeued > 0 {
		log.Printf("Recovery: re-queued %d stuck order(s)", requeued)
	}

	return nil
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

type recoveryOrder struct {
	OrderID   string
	UserID    string
	ProductID string
	Quantity  int
	CreatedAt time.Time
}

type recoveryOrderSource interface {
	LoadStuckOrders(ctx context.Context, threshold time.Duration) ([]recoveryOrder, error)
}

type recoveryOrdersQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const stuckOrdersQuery = `
	SELECT order_id, user_id, product_id, quantity, created_at
	FROM orders
	WHERE status = 'accepted'
	  AND created_at < NOW() - $1::interval
`

type pgxRecoveryOrderSource struct {
	db *pgx.Conn
}

func (source pgxRecoveryOrderSource) LoadStuckOrders(
	ctx context.Context,
	threshold time.Duration,
) ([]recoveryOrder, error) {
	return loadStuckOrders(ctx, source.db, threshold)
}

func loadStuckOrders(
	ctx context.Context,
	db recoveryOrdersQuerier,
	threshold time.Duration,
) ([]recoveryOrder, error) {
	rows, err := db.Query(ctx, stuckOrdersQuery, threshold.String())
	if err != nil {
		return nil, fmt.Errorf("query stuck orders: %w", err)
	}
	defer rows.Close()

	var orders []recoveryOrder

	for rows.Next() {
		var order recoveryOrder
		if err := rows.Scan(
			&order.OrderID,
			&order.UserID,
			&order.ProductID,
			&order.Quantity,
			&order.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan stuck order: %w", err)
		}

		orders = append(orders, order)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read stuck orders: %w", err)
	}

	return orders, nil
}

type orderDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type failureStatusDB interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgconn.CommandTag, error)
}

func parseOrderForFailure(body []byte) (OrderMessage, error) {
	var order OrderMessage

	if err := json.Unmarshal(body, &order); err != nil {
		return OrderMessage{}, fmt.Errorf("parse failed order: %w", err)
	}

	if !uuidPattern.MatchString(order.OrderID) ||
		order.UserID == "" ||
		order.ProductID == "" ||
		order.Quantity != 1 ||
		order.CreatedAt.IsZero() {
		return OrderMessage{}, errors.New("invalid order details for failure status")
	}

	return order, nil
}

func markOrderFailed(
	ctx context.Context,
	db failureStatusDB,
	order OrderMessage,
) error {
	if !uuidPattern.MatchString(order.OrderID) {
		return errors.New("cannot mark failed: invalid order ID")
	}

	if order.UserID == "" ||
		order.ProductID == "" ||
		order.Quantity != 1 ||
		order.CreatedAt.IsZero() {
		return errors.New("cannot mark failed: invalid order details")
	}

	result, err := db.Exec(ctx, `
		INSERT INTO orders (
			order_id, user_id, product_id, quantity, status, created_at
		)
		VALUES ($1, $2, $3, $4, 'failed', $5)
		ON CONFLICT (order_id) DO UPDATE
		SET status = 'failed'
		WHERE orders.user_id = EXCLUDED.user_id
		  AND orders.product_id = EXCLUDED.product_id
		  AND orders.quantity = EXCLUDED.quantity
		  AND orders.status = 'accepted'
	`,
		order.OrderID,
		order.UserID,
		order.ProductID,
		order.Quantity,
		order.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("mark order failed: %w", err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf(
			"order failure was not recorded: order %s is not eligible for failure transition",
			order.OrderID,
		)
	}

	return nil
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
	// RETURNING lets us detect whether the row was inserted this delivery.
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
		// Row already exists — check its current status.
		var currentStatus string
		err = tx.QueryRow(ctx, `
			SELECT status FROM orders WHERE order_id = $1
		`, order.OrderID).Scan(&currentStatus)
		if err != nil {
			return fmt.Errorf("fetch existing order status: %w", err)
		}

		if currentStatus == "completed" || currentStatus == "failed" {
			// Already at a terminal state; nothing to do.
			return tx.Commit(ctx)
		}

		// Status is still 'accepted': a prior run inserted it but crashed
		// before committing the inventory deduction and status update.
		// Verify the message fields match the stored record.
		var matches bool
		err = tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM orders
				WHERE order_id   = $1
				  AND user_id    = $2
				  AND product_id = $3
				  AND quantity   = $4
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

		// Determine whether inventory was deducted in the prior attempt.
		// processed_at is set in the same UPDATE that marks the order
		// 'completed', so its presence means inventory was already taken.
		var alreadyDecremented bool
		err = tx.QueryRow(ctx, `
			SELECT processed_at IS NOT NULL FROM orders WHERE order_id = $1
		`, order.OrderID).Scan(&alreadyDecremented)
		if err != nil {
			return fmt.Errorf("check processed_at for stuck order: %w", err)
		}

		if alreadyDecremented {
			// Inventory was taken; only the status commit was lost. Re-apply
			// it without touching stock.
			_, err = tx.Exec(ctx, `
				UPDATE orders
				SET status = 'completed'
				WHERE order_id = $1 AND status != 'completed'
			`, order.OrderID)
			if err != nil {
				return fmt.Errorf("re-complete stuck order: %w", err)
			}
			return tx.Commit(ctx)
		}

		// The prior attempt did not deduct inventory. Fall through to the
		// deduction block using the existing order record.
		insertedOrderID = order.OrderID

	} else if err != nil {
		return fmt.Errorf("insert order into PostgreSQL: %w", err)
	}

	// Deduct inventory. This runs for genuinely new orders (insertedOrderID
	// was just set from RETURNING) and for stuck orders whose prior attempt
	// did not reach the inventory UPDATE.
	_ = insertedOrderID // used only to distinguish the two cases above

	result, err := tx.Exec(ctx, `
		UPDATE inventory
		SET stock      = stock - $2,
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
		SET status       = 'completed',
		    processed_at = NOW()
		WHERE order_id   = $1
		  AND user_id    = $2
		  AND product_id = $3
		  AND quantity   = $4
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
