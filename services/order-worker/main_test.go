package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func validTestOrder() OrderMessage {
	return OrderMessage{
		OrderID:   "0f22b057-bc12-4e5b-a8fe-b21e43def041",
		UserID:    "duplicate-test-20261028",
		ProductID: "smartphone-001",
		Quantity:  1,
		CreatedAt: time.Date(2026, 10, 28, 3, 47, 22, 841001000, time.UTC),
	}
}

func TestProcessOrderInvalidJSON(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	err = processOrder(context.Background(), db, []byte("{invalid json"))
	var permanentErr *PermanentError
	if !errors.As(err, &permanentErr) {
		t.Fatalf("expected PermanentError, got %v", err)
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database interaction: %v", err)
	}
}

func TestProcessOrderNewOrderDeductsInventoryAndCompletes(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}

	db.ExpectBegin()

	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnRows(
			pgxmock.NewRows([]string{"order_id"}).AddRow(order.OrderID),
		)

	db.ExpectExec("UPDATE inventory").
		WithArgs(order.ProductID, order.Quantity).
		WillReturnResult(pgconn.NewCommandTag("UPDATE 1"))

	db.ExpectExec("UPDATE orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
		).
		WillReturnResult(pgconn.NewCommandTag("UPDATE 1"))

	db.ExpectCommit()

	if err := processOrder(context.Background(), db, body); err != nil {
		t.Fatalf("processOrder returned error: %v", err)
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func TestProcessOrderDuplicatePreservesCompletion(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}

	db.ExpectBegin()

	// INSERT conflicts → no row returned.
	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnRows(pgxmock.NewRows([]string{"order_id"}))

	// New: check the existing row's status first.
	db.ExpectQuery("SELECT status FROM orders WHERE order_id").
		WithArgs(order.OrderID).
		WillReturnRows(
			pgxmock.NewRows([]string{"status"}).AddRow("completed"),
		)

	// Already completed — commit the no-op; no inventory touch.
	db.ExpectCommit()

	if err := processOrder(context.Background(), db, body); err != nil {
		t.Fatalf("processOrder returned error: %v", err)
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func TestProcessOrderConflictingDuplicateRollsBack(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}

	db.ExpectBegin()

	// INSERT conflicts → no row returned.
	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnRows(pgxmock.NewRows([]string{"order_id"}))

	// New: check the existing row's status first — still 'accepted'.
	db.ExpectQuery("SELECT status FROM orders WHERE order_id").
		WithArgs(order.OrderID).
		WillReturnRows(
			pgxmock.NewRows([]string{"status"}).AddRow("accepted"),
		)

	// Field-match check: different user/product → returns false.
	db.ExpectQuery("SELECT EXISTS").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
		).
		WillReturnRows(
			pgxmock.NewRows([]string{"exists"}).AddRow(false),
		)

	db.ExpectRollback()

	err = processOrder(context.Background(), db, body)
	var permanentErr *PermanentError
	if !errors.As(err, &permanentErr) {
		t.Fatalf("expected PermanentError for conflicting duplicate, got %v", err)
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func TestProcessOrderDatabaseErrorRollsBack(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}

	db.ExpectBegin()

	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnError(errors.New("simulated database failure"))

	db.ExpectRollback()

	err = processOrder(context.Background(), db, body)
	if err == nil {
		t.Fatal("expected a database error")
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func TestProcessOrderInsufficientInventoryRollsBack(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}

	db.ExpectBegin()

	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnRows(
			pgxmock.NewRows([]string{"order_id"}).AddRow(order.OrderID),
		)

	db.ExpectExec("UPDATE inventory").
		WithArgs(order.ProductID, order.Quantity).
		WillReturnResult(pgconn.NewCommandTag("UPDATE 0"))

	db.ExpectRollback()

	err = processOrder(context.Background(), db, body)
	var permanentErr *PermanentError
	if !errors.As(err, &permanentErr) {
		t.Fatalf("expected PermanentError for insufficient inventory, got %v", err)
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func TestRetryCount(t *testing.T) {
	tests := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{name: "nil headers", headers: nil, want: 0},
		{name: "missing retry count", headers: amqp.Table{}, want: 0},
		{name: "int32 retry count", headers: amqp.Table{"x-retry-count": int32(2)}, want: 2},
		{name: "int64 retry count", headers: amqp.Table{"x-retry-count": int64(3)}, want: 3},
		{name: "unsupported type", headers: amqp.Table{"x-retry-count": "2"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryCount(tt.headers); got != tt.want {
				t.Fatalf("retryCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCopyHeadersDoesNotMutateOriginal(t *testing.T) {
	original := amqp.Table{
		"x-retry-count": int32(1),
		"existing":      "preserve-me",
	}

	copied := copyHeaders(original)
	copied["x-retry-count"] = int32(2)
	copied["new-header"] = "test"

	if original["x-retry-count"] != int32(1) {
		t.Fatal("copyHeaders modified the original retry count")
	}
	if _, exists := original["new-header"]; exists {
		t.Fatal("copyHeaders added a header to the original")
	}
	if original["existing"] != "preserve-me" {
		t.Fatal("copyHeaders changed an existing header")
	}
}

type fakePublisher struct {
	publishErr error
	called     bool
	exchange   string
	routingKey string
	message    amqp.Publishing
}

func (f *fakePublisher) Publish(
	exchange string,
	key string,
	mandatory bool,
	immediate bool,
	msg amqp.Publishing,
) error {
	f.called = true
	f.exchange = exchange
	f.routingKey = key
	f.message = msg
	return f.publishErr
}

func TestPublishAndConfirmAck(t *testing.T) {
	publisher := &fakePublisher{}
	confirms := make(chan amqp.Confirmation, 1)
	confirms <- amqp.Confirmation{Ack: true}

	err := publishAndConfirm(
		publisher, confirms,
		"flashsale.retry", "retry",
		[]byte(`{"test":true}`), nil,
	)

	if err != nil {
		t.Fatalf("expected successful publish, got: %v", err)
	}
	if !publisher.called {
		t.Fatal("expected publisher to be called")
	}
	if publisher.exchange != "flashsale.retry" ||
		publisher.routingKey != "retry" {
		t.Fatalf(
			"unexpected destination: %s / %s",
			publisher.exchange,
			publisher.routingKey,
		)
	}
	if publisher.message.DeliveryMode != amqp.Persistent {
		t.Fatal("expected persistent message")
	}
}

func TestPublishAndConfirmNack(t *testing.T) {
	publisher := &fakePublisher{}
	confirms := make(chan amqp.Confirmation, 1)
	confirms <- amqp.Confirmation{Ack: false}

	err := publishAndConfirm(
		publisher, confirms,
		"flashsale.dlx", "flashsale.orders",
		[]byte(`{"test":true}`), nil,
	)

	if err == nil {
		t.Fatal("expected error for negative confirmation")
	}
}

func TestPublishAndConfirmPublishError(t *testing.T) {
	publisher := &fakePublisher{
		publishErr: errors.New("simulated publish failure"),
	}
	confirms := make(chan amqp.Confirmation)

	err := publishAndConfirm(
		publisher, confirms,
		"flashsale.retry", "retry",
		[]byte(`{"test":true}`), nil,
	)

	if err == nil {
		t.Fatal("expected publish error")
	}
}

func TestPublishAndConfirmClosedChannel(t *testing.T) {
	publisher := &fakePublisher{}
	confirms := make(chan amqp.Confirmation)
	close(confirms)

	err := publishAndConfirm(
		publisher, confirms,
		"flashsale.retry", "retry",
		[]byte(`{"test":true}`), nil,
	)

	if err == nil {
		t.Fatal("expected error when confirmation channel closes")
	}
}

func TestMarkOrderFailedInsertsNewFailure(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()

	db.ExpectExec("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnResult(pgconn.NewCommandTag("INSERT 0 1"))

	if err := markOrderFailed(context.Background(), db, order); err != nil {
		t.Fatalf("markOrderFailed returned error: %v", err)
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func TestMarkOrderFailedRejectsInvalidOrder(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()
	order.OrderID = "invalid-id"

	if err := markOrderFailed(context.Background(), db, order); err == nil {
		t.Fatal("expected an error for invalid order ID")
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database interaction: %v", err)
	}
}

func TestMarkOrderFailedRejectsSkippedTransition(t *testing.T) {
	db, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}

	order := validTestOrder()

	db.ExpectExec("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnResult(pgconn.NewCommandTag("UPDATE 0"))

	err = markOrderFailed(context.Background(), db, order)
	if err == nil {
		t.Fatal("expected an error when the failure transition is skipped")
	}

	if err := db.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations not met: %v", err)
	}
}

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR to run Redis integration tests")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_TEST_PASSWORD"),
		DB:       15,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("test Redis unavailable: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func TestReleaseRedisReservationRestoresStockOnce(t *testing.T) {
	ctx := context.Background()
	rdb := testRedisClient(t)

	// Unique product ID: never touch the real product inventory.
	productID := "test-" + uuid.NewString()
	orderID := uuid.NewString()
	inventoryKey := "inventory:" + productID
	reservationKey := inventoryKey + ":reservation:" + orderID

	t.Cleanup(func() {
		_ = rdb.Del(ctx, inventoryKey, reservationKey).Err()
	})

	if err := rdb.Set(ctx, inventoryKey, 9, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, reservationKey, 1, 0).Err(); err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(OrderMessage{
		OrderID:   orderID,
		ProductID: productID,
		Quantity:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := releaseRedisReservation(ctx, rdb, body); err != nil {
		t.Fatalf("first release failed: %v", err)
	}

	stock, err := rdb.Get(ctx, inventoryKey).Int()
	if err != nil {
		t.Fatal(err)
	}
	if stock != 10 {
		t.Fatalf("stock after release = %d; want 10", stock)
	}

	// A second release must not restore stock a second time.
	if err := releaseRedisReservation(ctx, rdb, body); err != nil {
		t.Fatalf("repeated release failed: %v", err)
	}

	stock, err = rdb.Get(ctx, inventoryKey).Int()
	if err != nil {
		t.Fatal(err)
	}
	if stock != 10 {
		t.Fatalf("stock after repeated release = %d; want 10", stock)
	}
}

func TestReleaseRedisReservationRejectsMalformedJSON(t *testing.T) {
	ctx := context.Background()
	rdb := testRedisClient(t)

	if err := releaseRedisReservation(ctx, rdb, []byte("{")); err != nil {
		t.Fatalf("malformed JSON should not call Redis: %v", err)
	}
}

func TestReleaseRedisReservationMissingReservation(t *testing.T) {
	ctx := context.Background()
	rdb := testRedisClient(t)

	productID := "test-" + uuid.NewString()
	orderID := uuid.NewString()
	inventoryKey := "inventory:" + productID
	reservationKey := inventoryKey + ":reservation:" + orderID

	t.Cleanup(func() {
		_ = rdb.Del(ctx, inventoryKey, reservationKey).Err()
	})

	if err := rdb.Set(ctx, inventoryKey, 12, 0).Err(); err != nil {
		t.Fatalf("set test inventory: %v", err)
	}

	body, err := json.Marshal(OrderMessage{
		OrderID:   orderID,
		UserID:    "test-user",
		ProductID: productID,
		Quantity:  1,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}

	if err := releaseRedisReservation(ctx, rdb, body); err != nil {
		t.Fatalf("missing reservation should be harmless: %v", err)
	}

	stock, err := rdb.Get(ctx, inventoryKey).Int()
	if err != nil {
		t.Fatalf("read inventory: %v", err)
	}
	if stock != 12 {
		t.Fatalf("inventory changed without a reservation: got %d, want 12", stock)
	}
}

func TestReleaseRedisReservationUnavailableRedis(t *testing.T) {
	ctx := context.Background()

	// Use a local port that should not have a Redis server listening.
	rdb := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})
	defer rdb.Close()

	body, err := json.Marshal(OrderMessage{
		OrderID:   uuid.NewString(),
		UserID:    "test-user",
		ProductID: "test-product",
		Quantity:  1,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}

	if err := releaseRedisReservation(ctx, rdb, body); err == nil {
		t.Fatal("expected an error when Redis is unavailable")
	}
}
