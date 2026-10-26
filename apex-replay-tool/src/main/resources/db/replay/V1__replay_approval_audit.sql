-- This migration belongs ONLY to a separately provisioned operator audit database.
CREATE TABLE replay_audit_identity (id INTEGER PRIMARY KEY CHECK (id = 1), purpose TEXT NOT NULL);
INSERT INTO replay_audit_identity VALUES (1, 'apex-replay-audit-v1');

CREATE TABLE replay_plan (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    canonical_json TEXT NOT NULL,
    digest CHAR(64) NOT NULL,
    actor VARCHAR(500) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    item_count INTEGER NOT NULL CHECK (item_count BETWEEN 1 AND 10),
    status VARCHAR(32) NOT NULL CHECK (status IN ('PLANNED', 'APPROVED')),
    approval_actor VARCHAR(500),
    approval_reason VARCHAR(500),
    approval_at TIMESTAMPTZ,
    approval_digest CHAR(64),
    CHECK ((status = 'PLANNED' AND approval_at IS NULL AND approval_digest IS NULL)
        OR (status = 'APPROVED' AND approval_at IS NOT NULL AND approval_digest = digest
            AND approval_actor IS NOT NULL AND approval_reason IS NOT NULL))
);

CREATE TABLE replay_item (
    plan_id UUID NOT NULL REFERENCES replay_plan(id),
    origin_topic TEXT NOT NULL,
    origin_partition INTEGER NOT NULL CHECK (origin_partition >= 0),
    origin_offset BIGINT NOT NULL CHECK (origin_offset >= 0),
    target_group TEXT NOT NULL,
    event_id UUID NOT NULL,
    raw_sha256 CHAR(64) NOT NULL,
    raw_length INTEGER NOT NULL CHECK (raw_length > 0),
    dlt_topic TEXT NOT NULL,
    dlt_partition INTEGER NOT NULL,
    dlt_offset BIGINT NOT NULL,
    replay_topic TEXT NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'PLANNED' CHECK (state = 'PLANNED'),
    PRIMARY KEY (target_group, origin_topic, origin_partition, origin_offset, event_id),
    -- Conservative v1: even different source coordinates with the same business event
    -- require manual review; a second plan cannot silently schedule them.
    UNIQUE (target_group, event_id),
    UNIQUE (target_group, origin_topic, origin_partition, origin_offset)
);

CREATE TABLE replay_action (
    id BIGSERIAL PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES replay_plan(id),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    action VARCHAR(32) NOT NULL CHECK (action IN ('PLAN', 'APPROVAL')),
    actor VARCHAR(500) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    digest CHAR(64) NOT NULL
);

CREATE FUNCTION reject_replay_audit_change() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'replay audit records are immutable'; END $$;
CREATE TRIGGER replay_item_immutable BEFORE UPDATE OR DELETE ON replay_item
FOR EACH ROW EXECUTE FUNCTION reject_replay_audit_change();
CREATE TRIGGER replay_action_append_only BEFORE UPDATE OR DELETE ON replay_action
FOR EACH ROW EXECUTE FUNCTION reject_replay_audit_change();
CREATE TRIGGER replay_identity_immutable BEFORE UPDATE OR DELETE ON replay_audit_identity
FOR EACH ROW EXECUTE FUNCTION reject_replay_audit_change();
CREATE FUNCTION protect_replay_plan() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'replay plan cannot be deleted'; END IF;
    IF OLD.status <> 'PLANNED' OR NEW.status <> 'APPROVED'
       OR (to_jsonb(OLD) - ARRAY['status','approval_actor','approval_reason','approval_at','approval_digest'])
          IS DISTINCT FROM
          (to_jsonb(NEW) - ARRAY['status','approval_actor','approval_reason','approval_at','approval_digest'])
    THEN RAISE EXCEPTION 'only explicit first approval is allowed'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER replay_plan_protection BEFORE UPDATE OR DELETE ON replay_plan
FOR EACH ROW EXECUTE FUNCTION protect_replay_plan();
