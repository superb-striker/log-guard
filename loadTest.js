import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

const errorRate        = new Rate('errors');
const ingestionLatency = new Trend('ingestion_latency', true);
const piiHitCount      = new Counter('pii_payloads_sent');
const criticalCount    = new Counter('critical_logs_sent');

// Test configuration
//
// Stage breakdown:
//   0-30s    : ramp to 20 VUs  - warm up, let RabbitMQ + DB stabilise
//   30s-2m30 : hold 20 VUs     - measurement window (steady state)
//   2m30-3m  : ramp to 50 VUs  - spike to simulate burst traffic
//   3m-4m    : hold 50 VUs     - observe behaviour under spike
//   4m-4m30  : ramp down       - graceful wind-down
export const options = {
  stages: [
    { duration: '30s', target: 20 },
    { duration: '2m',  target: 20 },
    { duration: '30s', target: 50 },
    { duration: '1m',  target: 50 },
    { duration: '30s', target: 0  },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],   // 95th percentile under 500ms
    errors:            ['rate<0.05'],   // less than 5% error rate
    ingestion_latency: ['p(99)<1000'],  // 99th percentile under 1s
  },
};

const BASE_URL = 'http://localhost:8080';
const HEADERS  = { 'Content-Type': 'application/json' };

// Payload pools
//
// CRITICAL is kept in a separate pool and selected with 5% probability.
// This reflects realistic production alert rates and prevents webhook flooding.
//
// PII variety covers all redaction branches:
//   email, IPv4, credit card (Luhn-valid), Aadhaar (Verhoeff-valid),
//   Indian phone, Bearer JWT token.
//
// NOTE: traceId and spanId stamped per-request below - do not set here.
const NORMAL_TEMPLATES = [
  {
    service: 'payment-service',
    level: 'INFO',
    message: 'Transaction processed for user john@example.com from 192.168.1.55',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'auth-service',
    level: 'WARN',
    message: 'Failed login attempt from 10.0.0.42 for user priya@corp.in',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'order-service',
    level: 'ERROR',
    message: 'Payment declined for card 5500005555555559, retrying',
    metadata: { env: 'prod', region: 'us-east-1' },
    hasPii: true,
  },
  {
    service: 'kyc-service',
    level: 'INFO',
    // Verhoeff-valid Aadhaar - exercises AADHAAR redaction branch
    message: 'KYC verified for Aadhaar 2348 6578 9520, phone +91 98765 43210',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'kyc-service',
    level: 'INFO',
    // Second Verhoeff-valid Aadhaar for redaction coverage
    message: 'Duplicate KYC submission detected for Aadhaar 9999 5518 3433, flagging for review',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'gateway',
    level: 'DEBUG',
    message: 'Request received at edge node 172.16.0.1',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'auth-service',
    level: 'INFO',
    // Exercises Bearer JWT TOKEN redaction branch
    message: 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.signature used for session',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'order-service',
    level: 'INFO',
    message: 'Order dispatched successfully, no issues detected',
    metadata: { env: 'prod', region: 'us-east-1' },
    hasPii: false,
  },
  {
    service: 'inventory-service',
    level: 'WARN',
    message: 'Stock level below threshold for SKU-99821, triggering reorder',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: false,
  },
  {
    service: 'notification-service',
    level: 'ERROR',
    message: 'Failed to deliver SMS to +91 91234 56789 after 3 attempts',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
];

const CRITICAL_TEMPLATES = [
  {
    service: 'payment-service',
    level: 'CRITICAL',
    message: 'Database connection pool exhausted - all retries failed for user rahul@payments.in',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: true,
  },
  {
    service: 'auth-service',
    level: 'CRITICAL',
    message: 'JWT signing key rotation failed - all auth tokens invalid',
    metadata: { env: 'prod', region: 'ap-south-1' },
    hasPii: false,
  },
];

// Main VU loop
export default function () {
  // 5% CRITICAL, 95% normal - realistic production ratio
  const pool     = Math.random() < 0.05 ? CRITICAL_TEMPLATES : NORMAL_TEMPLATES;
  const template = pool[Math.floor(Math.random() * pool.length)];

  const payload = {
    service:  template.service,
    level:    template.level,
    message:  template.message,
    metadata: template.metadata,
    traceId:  `k6-${__VU}-${__ITER}`,
    spanId:   `span-${__VU}-${__ITER}`,
  };

  if (template.hasPii)               piiHitCount.add(1);
  if (template.level === 'CRITICAL') criticalCount.add(1);

  const res = http.post(
    `${BASE_URL}/api/logs`,
    JSON.stringify(payload),
    { headers: HEADERS }
  );

  ingestionLatency.add(res.timings.duration);

  const ok = check(res, {
    'status is 202':         (r) => r.status === 202,
    'response time < 200ms': (r) => r.timings.duration < 200,
  });

  errorRate.add(!ok);

  sleep(0.01);
}

// Teardown
export function teardown() {
  console.log('------------------------------------------------------');
  console.log('k6 run complete.');
  console.log('Check Spring logs for:');
  console.log('  "Batch size [100] reached"  -> S3 batch flush fired');
  console.log('  "S3 upload complete"         -> Parquet landed in S3');
  console.log('  "Athena partition repair"    -> Athena updated');
  console.log('  "CRITICAL log detected"      -> alert webhook triggered');
  console.log('  "Alert suppressed"           -> cooldown working correctly');
  console.log('------------------------------------------------------');
}