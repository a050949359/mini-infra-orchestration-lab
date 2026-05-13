import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('error_rate');
const enqueueDuration = new Trend('enqueue_duration_ms', true);

const BASE_URL = __ENV.API_URL || 'http://localhost:5000';
const RUN_ID = __ENV.RUN_ID || '';

export const options = {
  stages: [
    { duration: '30s', target: 20 },
    { duration: '60s', target: 50 },
    { duration: '30s', target: 100 },
    { duration: '60s', target: 100 },
    { duration: '20s', target: 0 },
  ],
  thresholds: {
    http_req_duration:        ['p(95)<500', 'p(99)<1000'],
    error_rate:               ['rate<0.01'],
    enqueue_duration_ms:      ['p(95)<300'],
  },
};

// ~10% of requests are intentionally malformed to exercise error paths
const ERROR_RATE_TARGET = 0.10;

const ACTIONS = ['generate_report', 'send_notification', 'process_data', 'export_csv', 'force_fail'];

const BAD_PAYLOADS = [
  '{}',                                                    // missing all fields
  JSON.stringify({ type: '', priority: 1, payload: { user_id: 1, action: 'x', data: 'x' } }),  // empty type
  JSON.stringify({ type: 'x', priority: 'high', payload: { user_id: 1, action: 'x', data: 'x' } }),  // priority not int
  JSON.stringify({ type: 'x', priority: 1, payload: { user_id: 'abc', action: 'x', data: 'x' } }),   // user_id not int
  JSON.stringify({ type: 'x', priority: 1, payload: null }),  // payload not object
  'not-json',                                              // invalid JSON
];

function randomPayload() {
  const body = {
    type: 'stress_test',
    priority: Math.floor(Math.random() * 5) + 1,
    payload: {
      user_id: Math.floor(Math.random() * 1000) + 1,
      action: ACTIONS[Math.floor(Math.random() * ACTIONS.length)],
      data: `vu-${__VU}-iter-${__ITER}`,
    },
  };
  if (RUN_ID) body.run_id = RUN_ID;
  return JSON.stringify(body);
}

export default function () {
  const headers = { 'Content-Type': 'application/json' };

  const isBadRequest = Math.random() < ERROR_RATE_TARGET;

  // --- enqueue ---
  const body = isBadRequest
    ? BAD_PAYLOADS[Math.floor(Math.random() * BAD_PAYLOADS.length)]
    : randomPayload();

  const enqRes = http.post(`${BASE_URL}/api/v1/jobs`, body, { headers });

  if (isBadRequest) {
    check(enqRes, { 'bad request: status 400': (r) => r.status === 400 });
    sleep(0.1);
    return;
  }

  const enqOk = check(enqRes, {
    'enqueue: status 202':  (r) => r.status === 202,
    'enqueue: has job_id':  (r) => r.json('job_id') !== undefined,
  });

  errorRate.add(!enqOk);
  enqueueDuration.add(enqRes.timings.duration);

  if (!enqOk) {
    sleep(0.5);
    return;
  }

  const jobId = enqRes.json('job_id');

  // --- poll status (simulate real client) ---
  sleep(0.2);

  const statusRes = http.get(`${BASE_URL}/api/v1/jobs/${jobId}`, { headers });

  check(statusRes, {
    'status: status 200': (r) => r.status === 200,
    'status: has status field': (r) => r.json('status') !== undefined,
  });

  sleep(0.1);
}
