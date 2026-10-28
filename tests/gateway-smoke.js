import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 1,
  iterations: 1,
};

const BASE_URL = "http://localhost:8088";

export default function () {
  const health = http.get(`${BASE_URL}/health`);

  check(health, {
    "gateway health returns 200": (r) => r.status === 200,
  });

  const invalidCheckout = http.post(
    `${BASE_URL}/api/checkout`,
    JSON.stringify({
      user_id: "k6-smoke-test",
      product_id: "smartphone-001",
      quantity: 0,
    }),
    {
      headers: { "Content-Type": "application/json" },
    }
  );

  check(invalidCheckout, {
    "invalid checkout is rejected with a client error": (r) =>
      r.status >= 400 && r.status < 500,
    "invalid checkout explains quantity requirement": (r) =>
      r.body.includes("quantity=1"),
  });

  sleep(1);
}
