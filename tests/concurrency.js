import http from "k6/http";
import { check } from "k6";
import { Counter, Rate } from "k6/metrics";

const acceptedReservations = new Counter("accepted_reservations");
const soldOutResponses = new Counter("sold_out_responses");
const unexpectedResponses = new Rate("unexpected_responses");

export const options = {
  scenarios: {
    simultaneous_checkouts: {
      executor: "per-vu-iterations",
      vus: 30,
      iterations: 1,
      maxDuration: "30s",
    },
  },
  thresholds: {
    unexpected_responses: ["rate<0.01"],
  },
};

export default function () {
  const payload = JSON.stringify({
    user_id: `concurrency-user-${__VU}-${__ITER}`,
    product_id: "concurrency-test-20261028b",
    quantity: 1,
  });

  const response = http.post("http://localhost:8080/checkout", payload, {
    headers: { "Content-Type": "application/json" },
    timeout: "10s",
  });

  const expected = response.status === 202 || response.status === 409;
  unexpectedResponses.add(!expected);

  if (response.status === 202) {
    acceptedReservations.add(1);
  } else if (response.status === 409) {
    soldOutResponses.add(1);
  }

  check(response, {
    "accepted or sold out": () => expected,
    "accepted response has accepted=true": (r) =>
      r.status !== 202 || JSON.parse(r.body).accepted === true,
  });
}
