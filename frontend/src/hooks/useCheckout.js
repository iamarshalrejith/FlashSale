
import { useCallback, useEffect, useRef, useState } from "react";
import { createCheckout, fetchOrderStatus } from "../services/api";

const PRODUCT_ID = "smartphone-001";
const TERMINAL_STATUSES = ["completed", "failed"];

export function useCheckout() {
  const [order, setOrder] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const requestLock = useRef(false);

  const buyNow = useCallback(async () => {
    if (requestLock.current) return;

    requestLock.current = true;
    setLoading(true);
    setMessage("");
    setError("");
    setOrder(null);

    try {
      const userId = `user-${crypto.randomUUID()}`;

      const data = await createCheckout({
        userId,
        productId: PRODUCT_ID,
        quantity: 1,
      });

      if (!data.order_id) {
        throw new Error(
          "Checkout response did not include an order ID. The order may have been accepted; check before retrying."
        );
      }

      setOrder({
        order_id: data.order_id,
        status: "accepted",
      });

      setMessage(data.message || "Your order was accepted.");
    } catch (err) {
      setError(
        err.message ||
          "Checkout failed. Check your order status before retrying."
      );
    } finally {
      requestLock.current = false;
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!order?.order_id || TERMINAL_STATUSES.includes(order.status)) {
      return undefined;
    }

    let cancelled = false;
    let timer;
    let delay = 1000;
    let failures = 0;
    const controller = new AbortController();

    async function poll() {
      try {
        const data = await fetchOrderStatus(
          order.order_id,
          controller.signal
        );

        if (cancelled) return;

        failures = 0;
        setOrder(data);

        if (TERMINAL_STATUSES.includes(data.status)) {
          return;
        }

        delay = Math.min(delay * 1.5, 5000);
      } catch (err) {
        if (cancelled || err.name === "AbortError") return;

        failures += 1;
        if (failures >= 5) {
          setError(
            "Unable to refresh order status. Your order may still be processing."
          );
          return;
        }

        delay = Math.min(delay * 2, 10000);
      }

      if (!cancelled) {
        timer = setTimeout(
          poll,
          delay + Math.random() * 250
        );
      }
    }

    timer = setTimeout(poll, delay);

    return () => {
      cancelled = true;
      clearTimeout(timer);
      controller.abort();
    };
  }, [order?.order_id, order?.status]);

  return {
    order,
    loading,
    message,
    error,
    buyNow,
  };
}
