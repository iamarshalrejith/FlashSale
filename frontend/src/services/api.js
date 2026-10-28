
export async function createCheckout({ userId, productId, quantity }) {
  const response = await fetch("/api/checkout", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      user_id: userId,
      product_id: productId,
      quantity,
    }),
  });

  const data = await response.json();

  if (!response.ok || !data.accepted) {
    throw new Error(
      data.message || data.error || "Checkout failed"
    );
  }

  return data;
}

export async function fetchOrderStatus(orderId, signal) {
  const response = await fetch(
    `/api/orders/${encodeURIComponent(orderId)}`,
    { signal }
  );

  if (!response.ok) {
    throw new Error(
      response.status === 404
        ? "Order not found"
        : "Unable to retrieve order status"
    );
  }

  return response.json();
}
