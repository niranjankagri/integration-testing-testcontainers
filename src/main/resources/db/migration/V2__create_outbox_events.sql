-- Events committed with their aggregate changes and published asynchronously by the outbox relay.

CREATE TABLE outbox_events (
    event_id     UUID         PRIMARY KEY,
    aggregate_id BIGINT       NOT NULL,
    topic        VARCHAR(100) NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_events_pending ON outbox_events (created_at, event_id)
    WHERE published_at IS NULL;
