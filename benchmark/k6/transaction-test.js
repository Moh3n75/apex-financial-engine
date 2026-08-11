import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const transactionLatency = new Trend('transaction_latency');
const transactionSuccess = new Counter('transaction_success');
const transactionFailure = new Counter('transaction_failure');

export const options = {
    scenarios: {
        transaction_test: {
            executor: 'constant-arrival-rate',

            // Requests per second
            rate: Number(__ENV.RATE || 1000),

            timeUnit: '1s',

            // Actual measurement duration
            duration: __ENV.DURATION || '60s',

            // Start with enough VUs
            preAllocatedVUs: Number(__ENV.PRE_ALLOCATED_VUS || 100),

            // Maximum VUs k6 may allocate
            maxVUs: Number(__ENV.MAX_VUS || 1000),
        },
    },

    thresholds: {
        http_req_failed: ['rate<0.01'],

        http_req_duration: [
            'p(95)<100',
            'p(99)<200',
        ],
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {

    const sourceAccount = __ENV.SOURCE_ACCOUNT || '81193';
    const destinationAccount = __ENV.DESTINATION_ACCOUNT || '27790456';

    const payload = JSON.stringify({
        sourceAccount,
        destinationAccount,
        amount: 10
    });

    const response = http.post(
        `${BASE_URL}/api/v1/transactions`,
        payload,
        {
            headers: {
                'Content-Type': 'application/json',
            },
        }
    );

    transactionLatency.add(response.timings.duration);

    const success = check(response, {
        'transaction successful': (r) =>
            r.status >= 200 && r.status < 300,
    });

    if (success) {
        transactionSuccess.add(1);
    } else {
        transactionFailure.add(1);
    }
}