package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	"flashsale/checkout-service/inventory"
	"flashsale/checkout-service/models"
	"flashsale/checkout-service/queue"
)

type reservationManager interface {
	Reserve(context.Context, string, string, int) (int64, error)
	Release(context.Context, string, string) (int64, error)
}

type orderPublisher interface {
	Publish([]byte) error
}

type CheckoutHandler struct {
	reserver  reservationManager
	publisher orderPublisher
}

func NewCheckoutHandler(
	reserver reservationManager,
	publisher orderPublisher,
) *CheckoutHandler {
	return &CheckoutHandler{
		reserver:  reserver,
		publisher: publisher,
	}
}

func (h *CheckoutHandler) releaseReservation(
	productID string,
	orderID string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := h.reserver.Release(ctx, productID, orderID)
	return err
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
		http.Error(
			w,
			"Provide user_id, product_id and quantity=1",
			http.StatusBadRequest,
		)
		return
	}

	// Generate the order ID before reserving inventory so that the
	// reservation belongs to this specific order.
	order := models.OrderMessage{
		OrderID:   uuid.NewString(),
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		CreatedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	remaining, err := h.reserver.Reserve(
		ctx,
		req.ProductID,
		order.OrderID,
		req.Quantity,
	)
	if err != nil {
		var reservationErr *inventory.ReservationError

		if errors.As(err, &reservationErr) {
			writeJSON(w, http.StatusConflict, models.CheckoutResponse{
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

	message, err := json.Marshal(order)
	if err != nil {
		if releaseErr := h.releaseReservation(
			req.ProductID,
			order.OrderID,
		); releaseErr != nil {
			log.Printf(
				"failed to release reservation after marshal error for order %s: %v",
				order.OrderID,
				releaseErr,
			)
		}

		writeJSON(w, http.StatusInternalServerError, models.CheckoutResponse{
			Accepted: false,
			Message:  "Failed to create order message",
		})
		return
	}

	if err := h.publisher.Publish(message); err != nil {
		log.Printf("RabbitMQ publish failed for order %s: %v", order.OrderID, err)
		var publishErr *queue.PublishError

		// The message may have reached RabbitMQ. Keep the reservation
		// because releasing it could allow the product to be oversold.
		if errors.As(err, &publishErr) && publishErr.Uncertain {
			writeJSON(w, http.StatusServiceUnavailable, models.CheckoutResponse{
				Accepted: false,
				Message:  "Order status is uncertain; check order status before retrying",
				OrderID:  order.OrderID,
			})
			return
		}
		

		// Release only when the publisher confirms it failed before
		// publishing the message.
		if errors.As(err, &publishErr) && publishErr.PrePublish {
			if releaseErr := h.releaseReservation(
				req.ProductID,
				order.OrderID,
			); releaseErr != nil {
				log.Printf(
					"failed to release reservation after pre-publish failure for order %s: %v",
					order.OrderID,
					releaseErr,
				)
			}
		}

		writeJSON(w, http.StatusServiceUnavailable, models.CheckoutResponse{
			Accepted: false,
			Message:  "Order could not be confirmed as queued; contact support before retrying",
			OrderID:  order.OrderID,
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
