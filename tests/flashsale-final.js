import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const accepted = new Counter('checkout_accepted');
const conflict = new Counter('checkout_conflict');
const limited = new Counter('checkout_rate_limited');
const serverErrors = new Counter('checkout_server_errors');
const other = new Counter('checkout_other');

export const options = {
  scenarios: {
    flashsale_load: {
      executor: 'constant-arrival-rate',
      rate: 1000,
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 300,
      maxVUs: 1500,
    },
  },
  thresholds: {
    dropped_iterations: ['count==0'],
  },
};

export default function () {
  const payload = JSON.stringify({
    user_id: `k6-${__VU}-${__ITER}-${Date.now()}`,
    product_id: 'flashsale-final-20261028-001',
    quantity: 1,
  });

  const response = http.post(
    `${__ENV.BASE_URL || 'http://localhost:8088'}/api/checkout`,
    payload,
    {
      headers: { 'Content-Type': 'application/json' },
      timeout: '10s',
      tags: { endpoint: 'checkout' },
    }
  );

  if (response.status === 202) accepted.add(1);
  else if (response.status === 409) conflict.add(1);
  else if (response.status === 429) limited.add(1);
  else if (response.status >= 500) serverErrors.add(1);
  else other.add(1);

  check(response, {
    'request received an HTTP response': (r) => r.status > 0,
  });
}
