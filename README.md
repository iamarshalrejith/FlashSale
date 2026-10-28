# FlashSale — High-Concurrency Checkout System

A flash-sale checkout system designed to reserve limited inventory safely during concurrent purchase requests. The project uses Go, Redis Lua scripting, RabbitMQ, PostgreSQL, Docker Compose, and k6.

## Architecture

1. **Checkout API (Go)** — validates checkout requests, reserves inventory, and publishes order messages.
2. **Redis** — stores inventory counters and user reservation keys.
3. **Lua reservation script** — atomically checks stock, prevents duplicate user reservations, and decrements inventory.
4. **RabbitMQ** — transports order messages asynchronously and supports retry and dead-letter queues.
5. **Order worker (Go)** — consumes order messages and writes order state to PostgreSQL using transactions.
6. **PostgreSQL** — stores order records, completion status, and processing timestamps.
7. **Nginx** — provides the configured HTTP entry point on port 8088.
8. **k6** — generates concurrent checkout requests and records test metrics.

## Prerequisites

- Docker Desktop with Docker Compose
- Go
- k6
- PowerShell on Windows

## Configuration

Create a local `.env` file in the project root. Set these variables to your own strong credentials:

```dotenv
POSTGRES_PASSWORD=replace_with_a_strong_password
REDIS_PASSWORD=replace_with_a_strong_password
RABBITMQ_PASSWORD=replace_with_a_strong_password
```

Do not commit `.env` or share its contents. The Compose configuration requires these values.

## Start infrastructure

From the project root:

```powershell
docker compose up -d
docker compose ps
```

Wait until PostgreSQL, Redis, and RabbitMQ are healthy before starting the Go services.

## Run the checkout API

Open a PowerShell terminal at the project root. Load the local environment variables:

```powershell
$envContent = Get-Content .\.env

foreach ($name in @("POSTGRES_PASSWORD", "RABBITMQ_PASSWORD", "REDIS_PASSWORD")) {
    $line = $envContent |
        Where-Object { $_ -match "^$name=" } |
        Select-Object -First 1

    if (-not $line) { throw "Missing $name in .env" }

    $value = $line.Substring($line.IndexOf("=") + 1).Trim().Trim('"').Trim("'")
    if ([string]::IsNullOrWhiteSpace($value)) { throw "$name is empty" }

    Set-Item -Path "Env:$name" -Value $value
}

$env:RABBITMQ_USER = "flashsale"
$env:RABBITMQ_HOST = "localhost:5672"
$env:REDIS_ADDR = "localhost:6379"
$env:HTTP_ADDR = ":8080"

cd .\services\checkout-service
go run .
```

Keep this terminal running.

## Run the order worker

Open a second PowerShell terminal at the project root. Load the same environment variables using the script above, then configure the database URL:

```powershell
$env:DATABASE_URL = "postgres://flashsale:$([uri]::EscapeDataString($env:POSTGRES_PASSWORD))@localhost:5432/flashsale"
$env:RABBITMQ_USER = "flashsale"
$env:RABBITMQ_HOST = "localhost:5672"
$env:REDIS_ADDR = "localhost:6379"

cd .\services\order-worker
go run .
```

Keep the worker terminal running. If the database URL or other connection settings differ in your implementation, use the configuration expected by the worker source.

## API

Health check:

```powershell
Invoke-RestMethod http://localhost:8080/health
```

Example checkout request:

```powershell
$body = @{
    user_id = "demo-user-001"
    product_id = "smartphone-001"
    quantity = 1
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
    -Uri http://localhost:8080/checkout `
    -ContentType "application/json" `
    -Body $body
```

A successful reservation returns HTTP 202. A sold-out or duplicate reservation returns HTTP 409.

## Concurrency test

The k6 test script is `tests/concurrency.js`.

Run:

```powershell
k6 run .\tests\concurrency.js
```

The recorded test used 30 concurrent checkout requests against a dedicated test product initialized with 20 units.

Observed results:

- 20 accepted reservations
- 10 expected sold-out responses
- 0 unexpected responses in the custom metric
- Final inventory: 0
- All 60 k6 response checks passed
- Average HTTP response time: 12.7 ms
- p95 HTTP response time: 19.6 ms

The k6 built-in `http_req_failed` metric counted the expected HTTP 409 responses as failures. This does not mean the custom concurrency test failed.

Both test runs were verified in PostgreSQL: each test product had 20 completed orders, for 40 completed test orders in total.

## Worker tests

From `services/order-worker`:

```powershell
go test ./...
go test -race ./...
go build ./...
```

These commands passed during development.

## RabbitMQ queues

The configured workflow uses:

- `flashsale.orders` — main order queue
- `flashsale.orders.retry` — delayed retry queue
- `flashsale.orders.dlq` — dead-letter queue
- `flashsale.retry` — retry exchange
- `flashsale.dlx` — dead-letter exchange

The verified queue state after the concurrency tests was:

- Main queue: 0 messages
- Retry queue: 0 messages
- DLQ: 1 preserved message from an earlier malformed-message test

Do not purge the DLQ during the demo.

## PostgreSQL verification

```powershell
docker compose exec postgres psql -U flashsale -d flashsale -c "SELECT product_id, status, COUNT(*) AS order_count FROM orders WHERE product_id IN ('concurrency-test-20261028', 'concurrency-test-20261028b') GROUP BY product_id, status ORDER BY product_id, status;"
```

Expected recorded result: 20 completed orders per test product.

## Demo checklist

1. Start Docker Compose and verify service health.
2. Start the checkout API and worker in separate terminals.
3. Demonstrate the API health endpoint.
4. Show a successful checkout request.
5. Run the k6 concurrency test against a fresh test inventory key.
6. Verify the final Redis inventory and completed PostgreSQL orders.
7. Show RabbitMQ main, retry, and dead-letter queue counts.
8. Run the worker unit tests and race detector.
9. Explain the atomic Redis reservation, asynchronous order processing, transaction handling, retry behavior, and DLQ.

## Limitations and follow-up validation

- The documented concurrency test validates inventory reservation under concurrent requests.
- Worker unit tests cover selected database and publisher error paths.
- A full live RabbitMQ redelivery/retry and database-failure recovery demonstration still requires separate validation.
- Monitoring and load testing beyond the recorded k6 run should only be claimed if actually configured and demonstrated.

## Security

- Keep `.env` out of version control.
- Do not paste credentials into logs, screenshots, or reports.
- Rotate credentials that have been exposed and update the matching service configuration.
- The current Compose setup is intended for local demonstration, not production deployment without additional security and operational hardening.

## Latest Verification — October 28, 2026

### End-to-end checkout

A live checkout was submitted through the Nginx gateway at `http://localhost:8088/api/checkout`.

Verified results:

- Checkout API accepted the request and queued the order.
- Redis reserved one unit of `smartphone-001`.
- The Go order worker processed the order.
- PostgreSQL persisted the order with status `completed` and a populated `processed_at` timestamp.
- PostgreSQL inventory decreased from 499 to 498.
- Redis inventory decreased from 483 to 482.

Diagnostic order ID: `f6b34c84-4496-4cb8-8365-a37be21c55db`

### Additional Go test verification

- Checkout service: `go test -race -count=1 ./...` passed.
- Order worker: `go test -race -count=1 ./...` passed.
- Order-status service: `go test -race -count=1 ./...` compiled successfully; no test files were present.

### Known issues and pending validation

- Redis and PostgreSQL inventory values are not reconciled.
- Two historical orders remain in `accepted` status and require investigation.
- The historical inventory discrepancy remains unexplained.
- Retry and dead-letter queue failure scenarios need further validation.
- The recorded 30-request concurrency test does not establish the 100,000-request target.
- Frontend bundle size, time to interactive, and gateway load-balancing performance still require measurement.

Do not reset inventory or purge RabbitMQ queues without preserving relevant test data.