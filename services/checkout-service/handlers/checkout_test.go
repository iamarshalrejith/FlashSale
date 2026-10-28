package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"flashsale/checkout-service/inventory"
	"flashsale/checkout-service/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakePublisher struct {
	err error
}

func (p *fakePublisher) Publish(_ []byte) error {
	return p.err
}

type checkoutResponse struct {
	Accepted bool   `json:"accepted"`
	OrderID  string `json:"order_id"`
	Message  string `json:"message"`
}

func newCheckoutTest(
	t *testing.T,
	publishErr error,
) (*CheckoutHandler, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	server.Set("inventory:smartphone-001", "2")

	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})

	reserver := inventory.NewReserver(client)
	publisher := &fakePublisher{err: publishErr}

	return NewCheckoutHandler(reserver, publisher), server
}

func performCheckout(
	t *testing.T,
	handler *CheckoutHandler,
) *httptest.ResponseRecorder {
	t.Helper()

	body := []byte(`{
		"user_id": "test-user-1",
		"product_id": "smartphone-001",
		"quantity": 1
	}`)

	req := httptest.NewRequest(
		http.MethodPost,
		"/checkout",
		bytes.NewReader(body),
	)
	recorder := httptest.NewRecorder()

	handler.Checkout(recorder, req)
	return recorder
}

func decodeCheckoutResponse(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
) checkoutResponse {
	t.Helper()

	var response checkoutResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode checkout response: %v; body: %s",
			err, recorder.Body.String())
	}

	if response.OrderID == "" {
		t.Fatalf("expected order_id in response; body: %s",
			recorder.Body.String())
	}

	return response
}

func assertInventory(
	t *testing.T,
	server *miniredis.Miniredis,
	orderID string,
	wantStock string,
	wantReserved bool,
) {
	t.Helper()

	stock, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("read inventory: %v", err)
	}

	if stock != wantStock {
		t.Errorf("remaining stock = %s, want %s", stock, wantStock)
	}

	reservationKey := "inventory:smartphone-001:reservation:" + orderID
	gotReserved := server.Exists(reservationKey)

	if gotReserved != wantReserved {
		t.Errorf("reservation %s exists = %v, want %v",
			reservationKey, gotReserved, wantReserved)
	}
}

func TestCheckoutSuccessKeepsReservation(t *testing.T) {
	handler, server := newCheckoutTest(t, nil)

	recorder := performCheckout(t, handler)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s",
			recorder.Code, http.StatusAccepted, recorder.Body.String())
	}

	response := decodeCheckoutResponse(t, recorder)
	if !response.Accepted {
		t.Fatal("expected checkout to be accepted")
	}

	assertInventory(t, server, response.OrderID, "1", true)
}

func TestCheckoutDefinitePublishFailureReleasesReservation(t *testing.T) {
	publishErr := &queue.PublishError{
		Err:        errors.New("publisher unavailable before publish"),
		Uncertain:  false,
		PrePublish: true,
	}

	handler, server := newCheckoutTest(t, publishErr)

	recorder := performCheckout(t, handler)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s",
			recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	response := decodeCheckoutResponse(t, recorder)

	// Stock is restored, and this order's reservation is removed.
	assertInventory(t, server, response.OrderID, "2", false)
}

func TestCheckoutUncertainPublishFailureKeepsReservation(t *testing.T) {
	publishErr := &queue.PublishError{
		Err:       errors.New("confirmation timed out"),
		Uncertain: true,
	}

	handler, server := newCheckoutTest(t, publishErr)

	recorder := performCheckout(t, handler)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s",
			recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	response := decodeCheckoutResponse(t, recorder)

	// Publishing may have succeeded, so keep the reservation.
	assertInventory(t, server, response.OrderID, "1", true)
}

func TestCheckoutNegativeAcknowledgementKeepsReservation(t *testing.T) {
	publishErr := &queue.PublishError{
		Err:       errors.New("RabbitMQ negatively acknowledged order message"),
		Uncertain: false,
	}

	handler, server := newCheckoutTest(t, publishErr)

	recorder := performCheckout(t, handler)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s",
			recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}

	response := decodeCheckoutResponse(t, recorder)

	// Preserve the reservation for this failure mode, as designed.
	assertInventory(t, server, response.OrderID, "1", true)
}

func TestSameUserCanHaveIndependentOrderReservations(t *testing.T) {
	handler, server := newCheckoutTest(t, nil)

	first := performCheckout(t, handler)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first checkout status = %d; body: %s",
			first.Code, first.Body.String())
	}
	firstResponse := decodeCheckoutResponse(t, first)

	second := performCheckout(t, handler)
	if second.Code != http.StatusAccepted {
		t.Fatalf("second checkout status = %d; body: %s",
			second.Code, second.Body.String())
	}
	secondResponse := decodeCheckoutResponse(t, second)

	if firstResponse.OrderID == secondResponse.OrderID {
		t.Fatal("expected unique order IDs")
	}

	assertInventory(t, server, firstResponse.OrderID, "0", true)
	assertInventory(t, server, secondResponse.OrderID, "0", true)
}

var _ orderPublisher = (*fakePublisher)(nil)
var _ reservationManager = (*inventory.Reserver)(nil)
