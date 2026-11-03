package com.apex.observability;

/** Metadata-only, best-effort diagnostics. Admission never determines business success. */
public interface OperationalObserver {
    enum Operation { OUTBOX_INTENT, OUTBOX_CLAIM, OUTBOX_CLAIM_ATTEMPT, OUTBOX_SEND, OUTBOX_ACK, OUTBOX_MARK, OUTBOX_MARK_ATTEMPT,
        OUTBOX_RETRY, OUTBOX_DEAD, INBOX_TRANSACTION, CONSUMER_ATTEMPT, ACK_INVOCATION,
        QUARANTINE, RPC_CLIENT, RPC_SERVER, QUERY, QUERY_QUEUE, QUERY_EXECUTION, COLLECTION }
    enum Outcome { OBSERVED, COMMITTED, ROLLED_BACK, DUPLICATE, SUCCEEDED, FAILED,
        DEADLINE_EXCEEDED, CANCELLED, UNKNOWN, STALE }
    record Sample(Operation operation, Outcome outcome, long durationNanos) {
        public Sample {
            if (operation == null || outcome == null || durationNanos < 0)
                throw new IllegalArgumentException("Invalid bounded operational sample");
        }
    }
    boolean tryRecord(Sample sample);
}
