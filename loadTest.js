import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const ingestionLatency = new Trend('ingestion_latency', true);

// Test configuration - three stages:
// ramp up -> sustained load -> ramp down
export const options = {
  stages: [
    { duration: '30s', target: 20  },   // ramp to 50 VUs
    { duration: '2m',  target: 20  },   // hold - this is your measurement window
    { duration: '30s', target: 50 },    // spike
    { duration: '1m',  target: 50 },    // hold spike
    { duration: '30s', target: 0   },   // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],    // 99th percentile under 500ms
    errors:            ['rate<0.05'],    // less than 1% errors
  },
};

const BASE_URL = 'http://localhost:8080';

// Realistic log payloads - mix of levels, some with PII to exercise redaction
const PAYLOADS = [
  {
    service: 'payment-service',
    level: 'INFO',
    message: 'Transaction processed for user john@example.com from 192.168.1.55',
    traceId: `trace-${Math.random()}`,
    metadata: { env: 'prod', region: 'ap-south-1' },
  },
  {
    service: 'auth-service',
    level: 'WARN',
    message: 'Failed login attempt from 10.0.0.42 for user priya@corp.in',
    traceId: `trace-${Math.random()}`,
    metadata: { env: 'prod' },
  },
  {
    service: 'order-service',
    level: 'ERROR',
    message: 'Payment declined for card 5500005555555559, retrying',
    traceId: `trace-${Math.random()}`,
    metadata: { env: 'prod', region: 'us-east-1' },
  },
  {
    service: 'kyc-service',
    level: 'INFO',
    message: 'KYC verified for Aadhaar 2348 6578 2146, phone +91 98765 43210',
    traceId: `trace-${Math.random()}`,
    metadata: { env: 'prod' },
  },
  {
    service: 'gateway',
    level: 'DEBUG',
    message: 'Request received at edge node 172.16.0.1',
    traceId: `trace-${Math.random()}`,
    metadata: { env: 'prod' },
  },
];

export default function () {
  const payload = PAYLOADS[Math.floor(Math.random() * PAYLOADS.length)];

  // Stamp a unique traceId per request
  payload.traceId = `k6-${__VU}-${__ITER}`;

  const res = http.post(
    `${BASE_URL}/api/logs`,
    JSON.stringify(payload),
    { headers: { 'Content-Type': 'application/json' } }
  );

  ingestionLatency.add(res.timings.duration);

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });

  errorRate.add(!ok);

  sleep(0.01);
}