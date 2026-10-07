// k6: POST jobs to jobq as fast as VUS allow (a burst), or at RATE per second.
//
//   N=10000 VUS=50 k6 run loadtest/enqueue.js            # burst
//   N=1000 RATE=20 k6 run loadtest/enqueue.js            # steady arrivals
//
// Each job is a noop that sleeps about SLEEP_MS (±50%, standing in for a push
// service's latency) and fails FAIL_RATE of the time (standing in for its errors).
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const N = Number(__ENV.N || 1000);
const VUS = Number(__ENV.VUS || 50);
const RATE = Number(__ENV.RATE || 0);
const SLEEP_MS = Number(__ENV.SLEEP_MS || 50);
const FAIL_RATE = Number(__ENV.FAIL_RATE || 0);
const URL = (__ENV.JOBQ_URL || 'http://localhost:18181') + '/jobs';
const RUN = __ENV.RUN || 'run';
const HEADERS = { headers: { Authorization: `Bearer ${__ENV.JOBQ_SECRET || 'load'}`, 'Content-Type': 'application/json' } };

export const options = {
  scenarios: {
    enqueue: RATE > 0
      ? { executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: `${Math.ceil(N / RATE)}s`,
          preAllocatedVUs: VUS, maxVUs: VUS * 4 }
      : { executor: 'shared-iterations', iterations: N, vus: VUS, maxDuration: '10m' },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: { checks: ['rate==1'] },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  if (i >= N) return; // the arrival-rate executor can overshoot by one tick
  const sleep = SLEEP_MS > 0 ? Math.round(SLEEP_MS * (0.5 + Math.random())) : 0;
  const body = JSON.stringify({
    kind: 'noop',
    payload: { sleep_ms: sleep, fail_rate: FAIL_RATE },
    idempotency_key: `${RUN}-${i}`,
    max_attempts: 5,
  });
  const res = http.post(URL, body, HEADERS);
  check(res, { '201 created': (r) => r.status === 201 });
}
