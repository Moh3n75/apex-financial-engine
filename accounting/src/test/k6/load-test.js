import http from 'k6/http';
import {check, sleep} from 'k6';
import { htmlReport } from 'https://raw.githubusercontent.com/benc-uk/k6-reporter/main/dist/bundle.js';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.1/index.js';

// 1. Configuration Options
export const optionsForStagingRequest = {
    stages: [
        {duration: '10s', target: 20}, // Ramp up to 20 VUs over 30 seconds
        {duration: '30s', target: 20}, // Stay at 20 VUs for 1 minute
        {duration: '5s', target: 0},  // Ramp down to 0 VUs
    ],
    thresholds: {
        http_req_duration: ['p(95)<200'], // 95% of HTTP requests must finish under 200ms
        http_req_failed: ['rate<0.01'],    // HTTP errors should be less than 1%
    },
};

export const options = {
    scenarios: {
        exact_20_concurrent: {
            executor: 'per-vu-iterations',
            vus: 10,         // 20 concurrent users
            iterations: 1,   // Each user performs exactly 1 iteration
            maxDuration: '10s',
        },
    },
};

// 2. Default Function (Executed by VUs in a loop)
export default function () {
    const url = 'http://localhost:8080/api/v1/transactions';

    // 1. Generate dynamic payload data per Virtual User iteration
    const payload = JSON.stringify({
        sourceAccount: 81193,
        destinationAccount: 27790456,
        amount: 100,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const response = http.post(url, payload, params);

    // Assertions
    check(response, {
        'status is 200': (r) => r.status === 200,
        'response body contains transaction id': (r) => r.body.includes('amount'),
    });

    // Pause briefly to simulate real user behavior between requests
    sleep(1);
}

// Custom summary handler executed when the test finishes
export function handleSummary(data) {
    return {
        'summary.html': htmlReport(data), // Saves a rich HTML report
        'summary.json': JSON.stringify(data), // Saves raw JSON summary
        stdout: textSummary(data, { indent: ' ', enableColors: true }), // Prints default summary to terminal
    };
}