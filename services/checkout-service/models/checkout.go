package models

type CheckoutRequest struct {
	UserID    string `json:"user_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type CheckoutResponse struct {
    Accepted  bool   `json:"accepted"`
    Message   string `json:"message"`
    Remaining int64  `json:"remaining,omitempty"`
    OrderID   string `json:"order_id,omitempty"`
}

