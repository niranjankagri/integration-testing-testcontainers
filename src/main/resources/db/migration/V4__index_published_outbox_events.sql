-- Published rows are removed by retention in published_at order, without scanning pending events.

CREATE INDEX outbox_events_published_retention
    ON outbox_events (published_at, event_id)
    WHERE published_at IS NOT NULL;
