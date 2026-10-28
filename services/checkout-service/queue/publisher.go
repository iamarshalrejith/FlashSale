package queue

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishError struct {
	Err       error
	Uncertain bool
}

func (e *PublishError) Error() string {
	return e.Err.Error()
}

func (e *PublishError) Unwrap() error {
	return e.Err
}

const (
	ordersQueue    = "flashsale.orders"
	poolSize       = 8
	confirmTimeout = 3 * time.Second
)

type publisherSlot struct {
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
	mu       sync.Mutex
}

type Publisher struct {
	conn   *amqp.Connection
	slots  chan *publisherSlot
	mu     sync.RWMutex
	closed bool
}

func NewPublisher(url string) (*Publisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}

	p := &Publisher{
		conn:  conn,
		slots: make(chan *publisherSlot, poolSize),
	}

	cleanup := func() {
		for len(p.slots) > 0 {
			slot := <-p.slots
			_ = slot.channel.Close()
		}
		_ = conn.Close()
	}

	// Declare the queues and dead-letter infrastructure once.
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open setup channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		ordersQueue, true, false, false, false,
		amqp.Table{"x-dead-letter-exchange": "flashsale.dlx"},
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare orders queue: %w", err)
	}

	_, err = ch.QueueDeclare(
		"flashsale.orders.dlq", true, false, false, false, nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare dead-letter queue: %w", err)
	}

	err = ch.ExchangeDeclare(
		"flashsale.dlx", "direct", true, false, false, false, nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare dead-letter exchange: %w", err)
	}

	err = ch.QueueBind(
		"flashsale.orders.dlq", ordersQueue, "flashsale.dlx", false, nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("bind dead-letter queue: %w", err)
	}
	_ = ch.Close()

	for i := 0; i < poolSize; i++ {
		slot, err := newPublisherSlot(conn)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create publisher channel %d: %w", i, err)
		}
		p.slots <- slot
	}

	return p, nil
}

func newPublisherSlot(conn *amqp.Connection) (*publisherSlot, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	confirmStream := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return &publisherSlot{
		channel:  ch,
		confirms: confirmStream,
	}, nil
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true

	for len(p.slots) > 0 {
		slot := <-p.slots
		_ = slot.channel.Close()
	}

	if p.conn != nil {
		_ = p.conn.Close()
	}
}

func (p *Publisher) Publish(body []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return fmt.Errorf("publisher is closed")
	}

	var slot *publisherSlot

	timer := time.NewTimer(confirmTimeout)
	defer timer.Stop()

	select {
	case slot = <-p.slots:
	case <-timer.C:
		return fmt.Errorf("timed out waiting for an available publisher channel")
	}

	healthy := false

	defer func() {
		if healthy {
			p.slots <- slot
			return
		}

		_ = slot.channel.Close()

		replacement, err := newPublisherSlot(p.conn)
		if err == nil {
			p.slots <- replacement
		}
	}()

	if err := slot.channel.Publish(
		"", ordersQueue, true, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	); err != nil {
		return &PublishError{
			Err:       fmt.Errorf("publish order message: %w", err),
			Uncertain: true,
		}
	}

	confirmTimer := time.NewTimer(confirmTimeout)
	defer confirmTimer.Stop()

	select {
	case confirmation, ok := <-slot.confirms:
		if !ok {
			return &PublishError{
				Err:       fmt.Errorf("RabbitMQ confirmation channel closed"),
				Uncertain: true,
			}
		}

		if !confirmation.Ack {
			return &PublishError{
				Err:       fmt.Errorf("RabbitMQ negatively acknowledged order message"),
				Uncertain: false,
			}
		}

		healthy = true
		return nil

	case <-confirmTimer.C:
		return &PublishError{
			Err:       fmt.Errorf("timed out waiting for RabbitMQ publisher confirmation"),
			Uncertain: true,
		}
	}
}
