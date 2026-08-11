#!/bin/bash

BASE_URL=${BASE_URL:-http://localhost:8080}

DURATION=${DURATION:-60s}

RATES=(100 500 1000 2000 5000 10000)

for RATE in "${RATES[@]}"
do
    echo "======================================"
    echo "Running benchmark at ${RATE} TPS"
    echo "======================================"

    k6 run \
        -e BASE_URL=$BASE_URL \
        -e RATE=$RATE \
        -e DURATION=$DURATION \
        -e PRE_ALLOCATED_VUS=200 \
        -e MAX_VUS=2000 \
        benchmark/k6/transaction-test.js

    echo ""
    echo "Finished ${RATE} TPS"
    echo ""

    sleep 10
done