import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const accepted = new Counter('checkout_accepted');
const soldOut = new Counter('checkout_conflict');
const rateLimited = new Counter('checkout_rate_limited');
const serverErrors = new Counter('checkout_server_errors');
const unexpected = new Counter('checkout_unexpected');

export const options = {
  scenarios: {
    checkout_smoke_load: {
      executor: 'constant-arrival-rate',
     rate: 25,
     timeUnit: '1s',
     duration: '10s',
     preAllocatedVUs: 25,
     maxVUs: 50,
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed: ['rate<0.10'],
  },
};

export default function () {
  const payload = JSON.stringify({
    user_id: `k6-${__VU}-${__ITER}-${Date.now()}`,
    product_id: 'loadtest-phone-001',
    quantity: 1,
  });

  const response = http.post(
    `${__ENV.BASE_URL || 'http://localhost:8088'}/api/checkout`,
    payload,
    {
      headers: { 'Content-Type': 'application/json' },
      timeout: '5s',
      tags: { endpoint: 'checkout' },
    }
  );

  if (response.status === 202) {
    accepted.add(1);
  } else if (response.status === 409) {
    soldOut.add(1);
  } else if (response.status === 429) {
    rateLimited.add(1);
  } else if (response.status >= 500) {
    serverErrors.add(1);
  } else {
    unexpected.add(1);
  }

  check(response, {
    'checkout returns an expected status': (r) =>
      [202, 409, 429].includes(r.status),
  });

  sleep(0.1);
}
