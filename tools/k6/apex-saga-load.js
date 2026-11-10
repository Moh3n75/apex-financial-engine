import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// APEX Order -> Inventory -> Payment -> Order load/consistency test.
// Send requests to the Gateway, never to ephemeral order-cell ports.
// Use ONLY against a disposable/staging environment with sufficient seeded stock.

function positiveInt(name, fallback) {
  const n = Number(__ENV[name] || fallback);
  if (!Number.isInteger(n) || n < 1) throw new Error(`${name} must be a positive integer`);
  return n;
}
function percent(name, fallback) {
  const n = Number(__ENV[name] ?? fallback);
  if (!Number.isFinite(n) || n < 0 || n > 100) throw new Error(`${name} must be 0..100`);
  return n;
}
function safeJson(response) {
  try { return response.json(); } catch (_) { return null; }
}

const MODE = __ENV.MODE || 'smoke';
if (!['smoke', 'load'].includes(MODE)) throw new Error('MODE must be smoke or load');
const BASE_URL = (__ENV.BASE_URL || 'http://127.0.0.1:8080').replace(/\/$/, '');
const PATH = '/api/v1/order-placements';
const PRODUCT_ID = __ENV.PRODUCT_ID || '11111111-1111-4111-8111-111111111111';
const QUANTITY = positiveInt('QUANTITY', 1);
const RATE = positiveInt('RATE', MODE === 'smoke' ? 1 : 50); // new orders per second
const PRE_VUS = positiveInt('PRE_VUS', MODE === 'smoke' ? 20 : 250);
const VERIFY_PERCENT = percent('VERIFY_PERCENT', MODE === 'smoke' ? 100 : 5);
const MAX_SAGA_MS = positiveInt('MAX_SAGA_MS', 30000);
const POLL_MS = positiveInt('POLL_MS', 500);
const EXPECTED_FINAL = __ENV.EXPECTED_FINAL || 'CONFIRMED'; // CONFIRMED or TERMINAL
if (!['CONFIRMED', 'TERMINAL'].includes(EXPECTED_FINAL)) {
  throw new Error('EXPECTED_FINAL must be CONFIRMED or TERMINAL');
}
const RUN_ID = (__ENV.RUN_ID || String(Date.now())).replace(/[^a-zA-Z0-9_-]/g, '');
const DURATION = __ENV.DURATION || '20s';

const ordersAttempted = new Counter('apex_orders_attempted');
const ordersCreated = new Counter('apex_orders_created');
const ordersUnknown = new Counter('apex_orders_unknown');
const ordersHttpRejected = new Counter('apex_orders_http_rejected');
const ordersConfirmed = new Counter('apex_orders_confirmed_sampled');
const ordersCancelled = new Counter('apex_orders_cancelled_sampled');
const ordersUnresolved = new Counter('apex_orders_unresolved_sampled');
const postSuccess = new Rate('apex_post_success_rate');
const sagaTerminal = new Rate('apex_saga_terminal_rate');
const sagaExpected = new Rate('apex_saga_expected_rate');
const orderCreateMs = new Trend('apex_order_create_ms', true);
const sagaEndToEndMs = new Trend('apex_saga_end_to_end_ms', true);
const statusPolls = new Counter('apex_status_poll_requests');

export const options = {
  scenarios: {
    order_saga: MODE === 'smoke'
      ? {
          executor: 'constant-arrival-rate',
          exec: 'createAndOptionallyVerify',
          rate: RATE,
          timeUnit: '1s',
          duration: DURATION,
          preAllocatedVUs: PRE_VUS,
          gracefulStop: '45s',
        }
      : {
          executor: 'ramping-arrival-rate',
          exec: 'createAndOptionallyVerify',
          startRate: 1,
          timeUnit: '1s',
          stages: [
            { duration: '45s', target: Math.max(1, Math.round(RATE * 0.25)) },
            { duration: '60s', target: Math.max(1, Math.round(RATE * 0.5)) },
            { duration: '120s', target: RATE },
            { duration: '45s', target: 0 },
          ],
          preAllocatedVUs: PRE_VUS,
          gracefulStop: '45s',
        },
  },
  thresholds: {
    apex_post_success_rate: ['rate>0.99'],
    'http_req_duration{endpoint:placement}': ['p(95)<1000', 'p(99)<2500'],
    dropped_iterations: ['count<1'],
    ...(VERIFY_PERCENT > 0 ? {
      apex_saga_terminal_rate: ['rate>0.95'],
      apex_saga_expected_rate: ['rate>0.95'],
    } : {}),
  },
};

export function setup() {
  const health = http.get(`${BASE_URL}/actuator/health/readiness`, {
    timeout: '5s',
    tags: { name: 'GET /actuator/health/readiness', endpoint: 'preflight' },
  });
  // The Gateway's readiness group validates that at least one Order endpoint is discoverable.
  if (health.status !== 200) {
    throw new Error(`Gateway not ready at ${BASE_URL}: HTTP ${health.status}; body=${String(health.body || '').slice(0, 300)}`);
  }
  return { ready: true };
}

export function createAndOptionallyVerify() {
  // Per-VU iteration counters are unique within each VU. Keep this unique across reruns.
  const reference = `k6-${RUN_ID}-${__VU}-${__ITER}`;
  const payload = JSON.stringify({
    externalReference: reference,
    description: 'APEX k6 Saga verification',
    items: [{ productId: PRODUCT_ID, quantity: QUANTITY }],
  });

  ordersAttempted.add(1);
  const started = Date.now();
  const createResponse = http.post(`${BASE_URL}${PATH}`, payload, {
    timeout: '8s',
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /api/v1/order-placements', endpoint: 'placement' },
  });
  orderCreateMs.add(Date.now() - started);

  const createdBody = safeJson(createResponse);
  const accepted = createResponse.status === 201 &&
    createdBody && typeof createdBody.orderId === 'string' &&
    createdBody.orderId.length > 0 &&
    typeof createdBody.status === 'string';

  postSuccess.add(Boolean(accepted));
  check(createResponse, {
    'POST 201 + orderId + status': () => Boolean(accepted),
  });

  if (!accepted) {
    // 5xx/timeout may be an UNKNOWN outcome: do not auto-retry a financial write.
    if (createResponse.status === 0 || createResponse.status >= 500) ordersUnknown.add(1);
    else ordersHttpRejected.add(1);
    if (__ITER < 2) {
      console.warn(`POST failed/unknown reference=${reference} HTTP=${createResponse.status} body=${String(createResponse.body || '').slice(0, 240)}`);
    }
    return;
  }

  ordersCreated.add(1);
  if (VERIFY_PERCENT === 0 || Math.random() * 100 >= VERIFY_PERCENT) return;

  // Sampled E2E validation; high load should avoid polling every order, which amplifies RPS.
  const orderId = createdBody.orderId;
  const deadline = started + MAX_SAGA_MS;
  let finalStatus = null;
  let lastSeenStatus = createdBody.status;
  let pollError = null;
  while (Date.now() < deadline) {
    sleep(POLL_MS / 1000);
    statusPolls.add(1);
    const response = http.get(`${BASE_URL}${PATH}/${encodeURIComponent(orderId)}`, {
      timeout: '8s',
      tags: { name: 'GET /api/v1/order-placements/{id}', endpoint: 'status' },
    });
    const body = safeJson(response);
    if (response.status !== 200 || !body || body.orderId !== orderId) {
      pollError = `HTTP=${response.status}`;
      continue;
    }
    lastSeenStatus = body.status;
    if (body.status === 'CONFIRMED' || body.status === 'CANCELLED') {
      finalStatus = body.status;
      break;
    }
  }

  const isTerminal = finalStatus !== null;
  const expected = EXPECTED_FINAL === 'TERMINAL' ? isTerminal : finalStatus === 'CONFIRMED';
  sagaTerminal.add(isTerminal);
  sagaExpected.add(expected);
  if (isTerminal) {
    sagaEndToEndMs.add(Date.now() - started);
    if (finalStatus === 'CONFIRMED') ordersConfirmed.add(1);
    else ordersCancelled.add(1);
  } else {
    ordersUnresolved.add(1);
    console.warn(`SAGA unresolved orderId=${orderId} lastStatus=${lastSeenStatus} pollError=${pollError || 'none'}`);
  }
}
