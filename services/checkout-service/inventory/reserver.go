package inventory

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed reserve.lua
var reserveScript string

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
	userID string,
	quantity int,
) (int64, error) {
	if productID == "" || userID == "" || quantity != 1 {
		return 0, fmt.Errorf("invalid reservation request")
	}

	key := "inventory:" + productID

	result, err := r.client.Eval(
		ctx,
		reserveScript,
		[]string{key},
		quantity,
		userID,
	).Result()
	if err != nil {
		return 0, fmt.Errorf("execute reservation script: %w", err)
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return 0, fmt.Errorf("unexpected reservation result")
	}

	status, ok := values[0].(int64)
	if !ok {
		return 0, fmt.Errorf("invalid reservation status")
	}

	message, ok := values[1].(string)
	if !ok {
		return 0, fmt.Errorf("invalid reservation response")
	}

	if status != 1 {
		return 0, &ReservationError{Code: message}
	}

	var remaining int64
	if _, err := fmt.Sscan(message, &remaining); err != nil {
		return 0, fmt.Errorf("invalid remaining inventory: %w", err)
	}

	return remaining, nil
}
