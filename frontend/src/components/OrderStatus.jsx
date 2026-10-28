
export default function OrderStatus({ order, message, error }) {
  if (!message && !error && !order) {
    return null;
  }

  return (
    <section
      className={`result-card ${error ? "result-error" : ""}`}
      aria-live="polite"
    >
      <h3>{error ? "Checkout needs attention" : "Your order"}</h3>

      {message && <p>{message}</p>}
      {error && <p>{error}</p>}

      {order?.order_id && (
        <>
          <p className="order-id">
            Order ID: {order.order_id}
          </p>

          <p>
            Status:{" "}
            <strong className="status-value">
              {order.status}
            </strong>
          </p>
        </>
      )}
    </section>
  );
}
