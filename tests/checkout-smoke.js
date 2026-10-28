import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 5,
  iterations: 5,
  thresholds: {
    checks: ["rate>0.89"],
    http_req_duration: ["p(95)<1000"],
  },
};

const BASE_URL = "http://localhost:8088";

export default function () {
  const userId = `k6-${__VU}-${__ITER}-${Date.now()}`;

  const response = http.post(
    `${BASE_URL}/api/checkout`,
    JSON.stringify({
      user_id: userId,
      product_id: "smartphone-001",
      quantity: 1,
    }),
    {
      headers: { "Content-Type": "application/json" },
      timeout: "5s",
    }
  );

  check(response, {
    "checkout returns an expected API status": (r) =>
      [200, 201, 202, 409, 429].includes(r.status),
    "checkout is not a gateway/server error": (r) =>
      r.status < 500,
  });

  sleep(1);
}
