package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	amqp "github.com/rabbitmq/amqp091-go"
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

	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnRows(pgxmock.NewRows([]string{"order_id"}))

	db.ExpectQuery("SELECT EXISTS").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
		).
		WillReturnRows(
			pgxmock.NewRows([]string{"exists"}).AddRow(true),
		)

	// Duplicate delivery must not deduct inventory again.
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

	db.ExpectQuery("INSERT INTO orders").
		WithArgs(
			order.OrderID,
			order.UserID,
			order.ProductID,
			order.Quantity,
			order.CreatedAt,
		).
		WillReturnRows(pgxmock.NewRows([]string{"order_id"}))

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
