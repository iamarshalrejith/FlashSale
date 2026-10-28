package inventory

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed reserve.lua
var reserveScript string

//go:embed release.lua
var releaseScript string

type Reserver struct {
	client *redis.Client
}

type ReservationError struct {
	Code string
}

func (e *ReservationError) Error() string {
	return e.Code
}

func NewReserver(client *redis.Client) *Reserver {
	return &Reserver{client: client}
}

func (r *Reserver) Reserve(
	ctx context.Context,
	productID string,
	orderID string,
	quantity int,
) (int64, error) {
	if productID == "" || orderID == "" || quantity != 1 {
		return 0, fmt.Errorf("invalid reservation request")
	}

	key := "inventory:" + productID

	result, err := r.client.Eval(
		ctx,
		reserveScript,
		[]string{key},
		quantity,
		orderID,
	).Result()
	if err != nil {
		return 0, fmt.Errorf("execute reservation script: %w", err)
	}

	return parseScriptResult(result, "reservation")
}

// Release restores inventory only for this exact order's reservation.
// Call only when it is safe to conclude that the order was not accepted.
func (r *Reserver) Release(
	ctx context.Context,
	productID string,
	orderID string,
) (int64, error) {
	if productID == "" || orderID == "" {
		return 0, fmt.Errorf("invalid release request")
	}

	key := "inventory:" + productID

	result, err := r.client.Eval(
		ctx,
		releaseScript,
		[]string{key},
		orderID,
	).Result()
	if err != nil {
		return 0, fmt.Errorf("execute release script: %w", err)
	}

	return parseScriptResult(result, "release")
}

func parseScriptResult(result interface{}, operation string) (int64, error) {
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return 0, fmt.Errorf("unexpected %s result", operation)
	}

	status, ok := values[0].(int64)
	if !ok {
		return 0, fmt.Errorf("invalid %s status", operation)
	}

	message, ok := values[1].(string)
	if !ok {
		return 0, fmt.Errorf("invalid %s response", operation)
	}

	if status != 1 {
		return 0, &ReservationError{Code: message}
	}

	var remaining int64
	if _, err := fmt.Sscan(message, &remaining); err != nil {
		return 0, fmt.Errorf("invalid remaining inventory after %s: %w", operation, err)
	}

	return remaining, nil
}
