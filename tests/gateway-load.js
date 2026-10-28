import http from "k6/http";
import { check } from "k6";

export const options = {
  stages: [
    { duration: "5s", target: 1 },
    { duration: "10s", target: 5 },
    { duration: "5s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<500"],
    checks: ["rate>0.99"],
  },
};

export default function () {
  const response = http.get("http://localhost:8088/health");

  check(response, {
    "gateway returns 200": (r) => r.status === 200,
    "gateway responds with healthy status": (r) =>
      r.json("status") === "ok",
  });
}
