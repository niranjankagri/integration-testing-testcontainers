-- A lease lets multiple relay instances claim different events and recover claims after a process crash.

ALTER TABLE outbox_events
    ADD COLUMN claim_token UUID,
    ADD COLUMN claimed_until TIMESTAMPTZ,
    ADD CONSTRAINT outbox_events_claim_pair CHECK ((claim_token IS NULL) = (claimed_until IS NULL));

CREATE INDEX outbox_events_unpublished_aggregate_order
    ON outbox_events (aggregate_id, created_at, event_id)
    WHERE published_at IS NULL;
