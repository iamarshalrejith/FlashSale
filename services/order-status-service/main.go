package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

type OrderStatusResponse struct {
	OrderID     string     `json:"order_id"`
	UserID      string     `json:"user_id"`
	ProductID   string     `json:"product_id"`
	Quantity    int        `json:"quantity"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
}

type Server struct {
	db *pgx.Conn
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	db, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Fatalf("Connect to PostgreSQL: %v", err)
	}
	defer db.Close(context.Background())

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Ping PostgreSQL: %v", err)
	}

	server := &Server{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("GET /orders/", server.getOrderStatus)

	httpServer := &http.Server{
		Addr:              ":" + envOr("PORT", "8081"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("Order Status API listening on :%s", strings.TrimPrefix(httpServer.Addr, ":"))
		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.db.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) getOrderStatus(w http.ResponseWriter, r *http.Request) {
	// Expected path: /orders/{order_id}
	orderID := strings.TrimPrefix(r.URL.Path, "/orders/")
	if orderID == "" || strings.Contains(orderID, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "A valid order ID is required",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var order OrderStatusResponse
	err := s.db.QueryRow(ctx, `
		SELECT order_id, user_id, product_id, quantity, status, created_at, processed_at
		FROM orders
		WHERE order_id = $1
	`, orderID).Scan(
		&order.OrderID,
		&order.UserID,
		&order.ProductID,
		&order.Quantity,
		&order.Status,
		&order.CreatedAt,
		&order.ProcessedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "Order not found",
			})
			return
		}

		log.Printf("Query order status failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "Unable to retrieve order status",
		})
		return
	}

	writeJSON(w, http.StatusOK, order)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("Write JSON response failed: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Keep the net/url import available for future URL validation/configuration.
var _ = url.URL{}
