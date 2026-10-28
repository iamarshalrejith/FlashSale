import http from 'k6/http';
import { Counter } from 'k6/metrics';

const statuses = new Counter('response_statuses');

export const options = {
  scenarios: {
    gateway_burst: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '3s',
      preAllocatedVUs: 50,
      maxVUs: 100,
    },
  },
};

export default function () {
  const response = http.post(
    'http://localhost:8088/api/checkout',
    '{}',
    {
      headers: { 'Content-Type': 'application/json' },
      timeout: '5s',
    }
  );

  statuses.add(1, { status: String(response.status) });
}
