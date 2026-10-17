-- Orders, and the ids of payment events already applied (so a redelivered event is applied only once).

CREATE TABLE orders (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer    VARCHAR(100) NOT NULL,
    sku         VARCHAR(40)  NOT NULL,
    quantity    INTEGER      NOT NULL CHECK (quantity BETWEEN 1 AND 10),
    status      VARCHAR(20)  NOT NULL CHECK (status IN ('PLACED', 'PAID', 'CANCELLED')),
    created_at  TIMESTAMPTZ  NOT NULL
);

CREATE INDEX orders_customer ON orders (customer);

CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
