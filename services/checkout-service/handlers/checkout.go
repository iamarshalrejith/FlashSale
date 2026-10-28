package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"time"

	"flashsale/checkout-service/inventory"
	"flashsale/checkout-service/models"
	"flashsale/checkout-service/queue"
)

type CheckoutHandler struct {
	reserver  *inventory.Reserver
	publisher *queue.Publisher
}

func NewCheckoutHandler(reserver *inventory.Reserver, publisher *queue.Publisher) *CheckoutHandler {
	return &CheckoutHandler{
		reserver:  reserver,
		publisher: publisher,
	}
}

func (h *CheckoutHandler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (h *CheckoutHandler) Checkout(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)

	var req models.CheckoutRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "Invalid JSON request", http.StatusBadRequest)
		return
	}

	if req.UserID == "" || req.ProductID == "" || req.Quantity != 1 {
		http.Error(w, "Provide user_id, product_id and quantity=1", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	remaining, err := h.reserver.Reserve(
		ctx,
		req.ProductID,
		req.UserID,
		req.Quantity,
	)
	if err != nil {
		var reservationErr *inventory.ReservationError

		if errors.As(err, &reservationErr) {
			status := http.StatusConflict

			if reservationErr.Code == "SOLD_OUT" {
				status = http.StatusConflict
			}

			writeJSON(w, status, models.CheckoutResponse{
				Accepted: false,
				Message:  reservationErr.Code,
			})
			return
		}

		writeJSON(w, http.StatusServiceUnavailable, models.CheckoutResponse{
			Accepted: false,
			Message:  "Reservation service temporarily unavailable",
		})
		return
	}

	order := models.OrderMessage{
		OrderID:   uuid.NewString(),
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		CreatedAt: time.Now().UTC(),
	}

	message, err := json.Marshal(order)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, models.CheckoutResponse{
			Accepted: false,
			Message:  "Failed to create order message",
		})
		return
	}

	if err := h.publisher.Publish(message); err != nil {
		var publishErr *queue.PublishError

		if errors.As(err, &publishErr) && publishErr.Uncertain {
			writeJSON(w, http.StatusServiceUnavailable, models.CheckoutResponse{
				Accepted: false,
				Message:  "Order status is uncertain; check order status before retrying",
			})
			return
		}

		writeJSON(w, http.StatusServiceUnavailable, models.CheckoutResponse{
			Accepted: false,
			Message:  "Order could not be confirmed as queued; contact support before retrying",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, models.CheckoutResponse{
		Accepted:  true,
		Message:   "Inventory reserved and order queued for processing",
		Remaining: remaining,
		OrderID:   order.OrderID,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
