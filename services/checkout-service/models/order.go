package models

import "time"

type OrderMessage struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}
