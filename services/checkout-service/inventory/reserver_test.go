package inventory

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestReserver(t *testing.T, stock int) (*Reserver, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	server.Set("inventory:smartphone-001", fmt.Sprint(stock))

	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return NewReserver(client), server
}

func TestConcurrentReservationsNeverOversell(t *testing.T) {
	const initialStock = 10
	const requests = 50

	reserver, server := newTestReserver(t, initialStock)

	var successes atomic.Int64
	var failures atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			userID := fmt.Sprintf("user-%d", i)
			_, err := reserver.Reserve(
				context.Background(),
				"smartphone-001",
				userID,
				1,
			)

			if err == nil {
				successes.Add(1)
			} else {
				failures.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if got := successes.Load(); got != initialStock {
		t.Errorf("successful reservations = %d, want %d", got, initialStock)
	}

	if got := failures.Load(); got != requests-initialStock {
		t.Errorf("failed reservations = %d, want %d", got, requests-initialStock)
	}

	got, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("could not read test inventory: %v", err)
	}
	if got != "0" {
		t.Errorf("remaining stock = %s, want 0", got)
	}
}

func TestDuplicateUserCannotReserveTwice(t *testing.T) {
	reserver, server := newTestReserver(t, 2)

	_, err := reserver.Reserve(
		context.Background(), "smartphone-001", "same-user", 1,
	)
	if err != nil {
		t.Fatalf("first reservation failed: %v", err)
	}

	_, err = reserver.Reserve(
		context.Background(), "smartphone-001", "same-user", 1,
	)
	if err == nil {
		t.Fatal("expected duplicate reservation to fail")
	}

	got, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("could not read test inventory: %v", err)
	}
	if got != "1" {
		t.Errorf("remaining stock = %s, want 1", got)
	}
}

func TestReservationRejectsInvalidQuantity(t *testing.T) {
	reserver, server := newTestReserver(t, 2)

	_, err := reserver.Reserve(
		context.Background(), "smartphone-001", "user-invalid", 0,
	)
	if err == nil {
		t.Fatal("expected invalid quantity to fail")
	}

	got, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("could not read test inventory: %v", err)
	}
	if got != "2" {
		t.Errorf("remaining stock = %s, want 2", got)
	}
}

func TestReservationRejectsEmptyProductOrUser(t *testing.T) {
	reserver, server := newTestReserver(t, 2)

	tests := []struct {
		name      string
		productID string
		userID    string
	}{
		{name: "empty product", productID: "", userID: "user-1"},
		{name: "empty user", productID: "smartphone-001", userID: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := reserver.Reserve(
				context.Background(), tt.productID, tt.userID, 1,
			)
			if err == nil {
				t.Fatal("expected invalid request to fail")
			}
		})
	}

	got, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("could not read test inventory: %v", err)
	}
	if got != "2" {
		t.Errorf("remaining stock = %s, want 2", got)
	}
}

func TestReleaseRestoresReservedInventory(t *testing.T) {
	reserver, server := newTestReserver(t, 2)

	_, err := reserver.Reserve(
		context.Background(), "smartphone-001", "release-user", 1,
	)
	if err != nil {
		t.Fatalf("reservation failed: %v", err)
	}

	// Execute the release script directly against temporary Redis.
	script, err := os.ReadFile("release.lua")
	if err != nil {
		t.Fatalf("could not read release.lua: %v", err)
	}

	result, err := reserver.client.Eval(
		context.Background(),
		string(script),
		[]string{"inventory:smartphone-001"},
		"release-user",
	).Result()
	if err != nil {
		t.Fatalf("release script failed: %v", err)
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 2 || values[0] != int64(1) {
		t.Fatalf("unexpected release result: %#v", result)
	}

	got, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("could not read inventory: %v", err)
	}
	if got != "2" {
		t.Errorf("remaining inventory = %s, want 2", got)
	}
}

func TestReleaseCannotRestoreSameReservationTwice(t *testing.T) {
	reserver, server := newTestReserver(t, 2)

	_, err := reserver.Reserve(
		context.Background(), "smartphone-001", "release-once-user", 1,
	)
	if err != nil {
		t.Fatalf("reservation failed: %v", err)
	}

	script, err := os.ReadFile("release.lua")
	if err != nil {
		t.Fatalf("could not read release.lua: %v", err)
	}

	runRelease := func() (interface{}, error) {
		return reserver.client.Eval(
			context.Background(),
			string(script),
			[]string{"inventory:smartphone-001"},
			"release-once-user",
		).Result()
	}

	if _, err := runRelease(); err != nil {
		t.Fatalf("first release failed: %v", err)
	}

	result, err := runRelease()
	if err != nil {
		t.Fatalf("second release execution failed: %v", err)
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 2 || values[0] != int64(0) {
		t.Fatalf("expected NO_RESERVATION on second release, got %#v", result)
	}

	got, err := server.Get("inventory:smartphone-001")
	if err != nil {
		t.Fatalf("could not read inventory: %v", err)
	}
	if got != "2" {
		t.Errorf("remaining inventory = %s, want 2", got)
	}
}
