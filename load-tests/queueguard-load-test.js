import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const admitted = new Counter('queueguard_admitted');
const rateLimited = new Counter('queueguard_rate_limited');

// Two scenarios run in parallel against the same api-service instance:
//
// 1. throughput: many distinct users, each well under their own 100/min
//    limit, so this measures raw system capacity (rate limiter + Redis
//    Stream enqueue) rather than admission-control behavior.
//
// 2. rate_limit_enforcement: one fixed user hammered continuously, to
//    prove the sliding-window limit actually engages under real load
//    and not just in a single curl burst.
export const options = {
  scenarios: {
    throughput: {
      executor: 'ramping-vus',
      exec: 'throughputScenario',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 25 },
        { duration: '30s', target: 50 },
        { duration: '15s', target: 0 },
      ],
    },
    rate_limit_enforcement: {
      executor: 'constant-vus',
      exec: 'rateLimitScenario',
      vus: 5,
      duration: '30s',
      startTime: '5s',
    },
  },
  thresholds: {
    'http_req_duration{scenario:throughput}': ['p(95)<500'],
    'http_req_failed{scenario:throughput}': ['rate<0.01'],
    queueguard_rate_limited: ['count>0'],
  },
};

function postJob(userId, tier) {
  const res = http.post(
    `${BASE_URL}/api/jobs`,
    JSON.stringify({ payload: 'k6-load-test' }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-User-Id': userId,
        'X-User-Tier': tier,
      },
    },
  );

  if (res.status === 429) {
    rateLimited.add(1);
  } else {
    admitted.add(1);
  }

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
  });

  return res;
}

export function throughputScenario() {
  // Unique-ish user per VU+iteration keeps each identity under its own
  // 100 req/min ceiling, isolating raw throughput from rate limiting.
  const userId = `loadtest-${__VU}-${__ITER}`;
  postJob(userId, __VU % 4 === 0 ? 'PREMIUM' : 'FREE');
}

export function rateLimitScenario() {
  postJob('loadtest-fixed-user', 'FREE');
}
