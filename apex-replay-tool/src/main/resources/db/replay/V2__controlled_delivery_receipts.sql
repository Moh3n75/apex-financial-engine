-- Tool-owned audit DB only. Immutable V1 plans/items/approvals remain untouched.
ALTER TABLE replay_item ADD CONSTRAINT replay_item_plan_event UNIQUE (plan_id,event_id);
CREATE TABLE replay_run (
    plan_id UUID PRIMARY KEY REFERENCES replay_plan(id),
    claim_id UUID NOT NULL UNIQUE,
    actor VARCHAR(500) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    cause_resolved VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TRIGGER replay_run_immutable BEFORE UPDATE OR DELETE ON replay_run
FOR EACH ROW EXECUTE FUNCTION reject_replay_audit_change();

CREATE TABLE replay_delivery (
    plan_id UUID NOT NULL,
    event_id UUID NOT NULL,
    claim_id UUID NOT NULL REFERENCES replay_run(claim_id),
    marker TEXT NOT NULL UNIQUE,
    state VARCHAR(32) NOT NULL CHECK (state IN ('PUBLISHING','PUBLISH_UNKNOWN','PUBLISHED')),
    destination_topic TEXT NOT NULL,
    destination_partition INTEGER NOT NULL CHECK (destination_partition >= 0),
    destination_offset BIGINT,
    diagnostic_code VARCHAR(80),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(plan_id,event_id),
    FOREIGN KEY(plan_id,event_id) REFERENCES replay_item(plan_id,event_id),
    CHECK ((state='PUBLISHED' AND destination_offset IS NOT NULL AND destination_offset >= 0)
        OR (state<>'PUBLISHED' AND destination_offset IS NULL))
);
CREATE FUNCTION protect_replay_delivery() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'delivery audit cannot be deleted'; END IF;
    IF OLD.state='PUBLISHED'
       OR NEW.state NOT IN ('PUBLISH_UNKNOWN','PUBLISHED')
       OR (to_jsonb(OLD)-ARRAY['state','destination_offset','diagnostic_code','updated_at'])
          IS DISTINCT FROM (to_jsonb(NEW)-ARRAY['state','destination_offset','diagnostic_code','updated_at'])
    THEN RAISE EXCEPTION 'invalid delivery transition'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER replay_delivery_protection BEFORE UPDATE OR DELETE ON replay_delivery
FOR EACH ROW EXECUTE FUNCTION protect_replay_delivery();

ALTER TABLE replay_action DROP CONSTRAINT replay_action_action_check;
ALTER TABLE replay_action ADD CONSTRAINT replay_action_kind CHECK
    (action IN ('PLAN','APPROVAL','EXECUTION','PUBLISH_INTENT','PUBLISHED','PUBLISH_UNKNOWN','RECONCILED','NO_EVIDENCE'));
ALTER TABLE replay_action ADD COLUMN event_id UUID;
ALTER TABLE replay_action ADD COLUMN claim_id UUID;
ALTER TABLE replay_action ADD COLUMN destination_topic TEXT;
ALTER TABLE replay_action ADD COLUMN destination_partition INTEGER;
ALTER TABLE replay_action ADD COLUMN destination_offset BIGINT;
ALTER TABLE replay_action ADD COLUMN diagnostic_code VARCHAR(80);
