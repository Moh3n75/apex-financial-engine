-- OPT-IN OWNER REVIEW ONLY. Run each statement in autocommit, NEVER in Flyway/default startup.
-- Invalid same-name index must be inspected/dropped CONCURRENTLY and rebuilt; no IF NOT EXISTS shortcut.
SET lock_timeout = '1s';
SET statement_timeout = '120s';
CREATE INDEX CONCURRENTLY ops03d_outbox_event_recent ON public.outbox_event (created_at DESC, id DESC);
CREATE INDEX CONCURRENTLY ops03d_processed_event_recent ON public.processed_event (consumer_group, processed_at DESC, event_id DESC);
CREATE INDEX CONCURRENTLY ops03d_payment_authorizations_recent ON public.payment_authorizations (created_at DESC, order_id DESC);
RESET statement_timeout;
RESET lock_timeout;
