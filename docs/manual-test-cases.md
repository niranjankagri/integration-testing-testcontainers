# Manual test cases

The integration test cases this project automates on the order service, written as a tester would
run them by hand against a running service with its real PostgreSQL and Kafka. Every case has an ID;
the JUnit method that covers it starts with the same ID (`tc306_aRedeliveredEventIsAppliedOnlyOnce`),
so a failure in the report leads straight to the case below, and one case runs with
`mvnw test -Dtest=PaymentEventsTest#tc306*`.

| ID range | Area | Automated in |
|---|---|---|
| TC-201 to TC-206 | GraphQL API | `OrderGraphQlApiTest` |
| TC-301 to TC-312 | Events: OrderPlaced (outbox), payment events, crash recovery, consumer lag | `OrderPlacedEventTest`, `PaymentEventsTest`, `OutboxCrashRecoveryTest`, `ConsumerLagTest` |
| TC-401 to TC-406 | Database: migrations, constraints, outbox store | `DatabaseMigrationTest` |
| TC-501 to TC-505 | Operations: health, metrics, consumer performance | `ObservabilityEndpointTest`, `PaymentConsumerPerformanceTest` (tag `performance`) |

Not listed: `KafkaConfigTest`, a unit test of which failures are retried.

## Setup

- Docker running. Start the service with its containers: `mvnw.cmd spring-boot:test-run` (PostgreSQL 18
  and Kafka via Testcontainers; the log shows their ports). GraphQL at `/graphql` (GraphiQL in the
  browser), Actuator at `/actuator/health` and `/actuator/prometheus`.
- Kafka tools: `kafka-console-producer` / `kafka-console-consumer` in the Kafka container
  (`docker exec`), topics `orders.placed`, `payments.completed`, `payments.completed.DLT`.
  Payment event: `{"eventId":"<uuid>","orderId":<id>,"result":"SUCCEEDED"}` (or `FAILED`), key = order id.
- SQL: `psql` in the PostgreSQL container; tables `orders`, `outbox_events`, `processed_events`.
- Place an order: `mutation { placeOrder(input: {customer: "c-1", sku: "SKU-COFFEE", quantity: 2}) { id status } }`.

## Summary

| ID | Title | Type | Priority |
|---|---|---|---|
| TC-201 | A placed order is stored and read back with status PLACED | API | High |
| TC-202 | `orders(customer)` lists only that customer's orders, newest first | API / Security | High |
| TC-203 | An unknown order id gives `null`, not an error | API | Medium |
| TC-204 | Invalid input: one BAD_REQUEST naming every broken field, nothing stored | API / Negative | High |
| TC-205 | A missing required field is rejected by the schema | API / Negative | Medium |
| TC-206 | An unknown field is rejected by the schema | API / Negative | Low |
| TC-301 | Placing an order publishes OrderPlaced keyed by the order id | Events | High |
| TC-302 | A rolled-back order leaves no outbox event | Events / Consistency | High |
| TC-303 | An invalid order publishes nothing | Events / Negative | Medium |
| TC-304 | A successful payment marks the order PAID | Events | High |
| TC-305 | A failed payment cancels the order | Events | High |
| TC-306 | A redelivered event is applied only once | Events / Idempotency | High |
| TC-307 | Broken JSON goes to the dead-letter topic and does not block later events | Events / Negative | High |
| TC-308 | An event without required fields goes to the dead-letter topic | Events / Negative | Medium |
| TC-309 | An event for an unknown order is retried, then dead-lettered | Events / Negative | Medium |
| TC-310 | A dead-lettered payment can be replayed once its order exists | Events / Operations | Medium |
| TC-311 | After a relay crash, a restarted instance republishes the event | Events / Resilience | High |
| TC-312 | The consumer-lag gauge counts waiting payments and returns to 0 | Events / Monitoring | Medium |
| TC-401 | All database migrations are applied | Database | High |
| TC-402 | The database refuses a quantity outside 1-10 | Database | Medium |
| TC-403 | The database refuses an unknown status | Database | Medium |
| TC-404 | A processed event id can be stored only once | Database / Idempotency | High |
| TC-405 | Outbox claims are exclusive, recoverable and ordered per order | Database / Concurrency | High |
| TC-406 | Outbox retention deletes only old published rows | Database | Medium |
| TC-501 | Health endpoint reports the application status | Operations | High |
| TC-502 | Prometheus endpoint exports metrics | Operations | Medium |
| TC-503 | The environment endpoint is not exposed | Operations / Security | High |
| TC-504 | One instance processes a payment backlog above the floor | Performance | Medium |
| TC-505 | Rebalances during processing lose nothing and apply nothing twice | Performance / Resilience | High |

---

## GraphQL API (`OrderGraphQlApiTest`)

Use a new customer name per case, so other data cannot interfere.

### TC-201 A placed order is stored and read back with status PLACED

| # | Step | Expected result |
|---|---|---|
| 1 | `placeOrder(input: {customer: "<new>", sku: "SKU-COFFEE", quantity: 2}) { id }` | No errors; an id |
| 2 | `order(id: <id>) { id customer sku quantity status }` | The same customer, `SKU-COFFEE`, `2`, `PLACED` |

### TC-202 `orders(customer)` lists only that customer's orders, newest first

| # | Step | Expected result |
|---|---|---|
| 1 | Place SKU-A, then SKU-B for customer X; place SKU-C for another customer | Three ids |
| 2 | `orders(customer: "X") { id }` | Exactly [SKU-B's id, SKU-A's id] |

### TC-203 An unknown order id gives `null`, not an error

| # | Step | Expected result |
|---|---|---|
| 1 | `{ order(id: "999999999") { id } }` | No errors; `order: null` |

### TC-204 Invalid input: one BAD_REQUEST naming every broken field, nothing stored

| # | Step | Expected result |
|---|---|---|
| 1 | `placeOrder` with customer `" "`, sku `SKU-A`, quantity `11` | One error, type BAD_REQUEST, message "customer: must not be blank; quantity: must be less than or equal to 10"; `data` is null |
| 2 | `SELECT count(*) FROM orders WHERE sku='SKU-A' AND quantity=11` | `0` |

### TC-205 A missing required field is rejected by the schema

| # | Step | Expected result |
|---|---|---|
| 1 | `mutation { placeOrder(input: {customer: "x", sku: "y"}) { id } }` (no quantity) | A validation error naming `quantity`; nothing stored |

### TC-206 An unknown field is rejected by the schema

| # | Step | Expected result |
|---|---|---|
| 1 | `{ order(id: "1") { id price } }` | A validation error naming `price` |

## Events (`OrderPlacedEventTest`, `PaymentEventsTest`, `OutboxCrashRecoveryTest`, `ConsumerLagTest`)

### TC-301 Placing an order publishes OrderPlaced keyed by the order id

| # | Step | Expected result |
|---|---|---|
| 1 | Consume `orders.placed`; place an order (SKU-TEA, quantity 3) | A record whose key is the order id; JSON with `orderId`, `customer`, `sku` `SKU-TEA`, `quantity` `3`, a UUID `eventId` |
| 2 | `SELECT published_at FROM outbox_events WHERE event_id = '<eventId>'` | Set (within 10 s) |

### TC-302 A rolled-back order leaves no outbox event

Needs code or a debugger: place an order inside a transaction that then fails (the test forces the
rollback).

| # | Step | Expected result |
|---|---|---|
| 1 | Place an order in a transaction, then roll it back | No row in `orders` and no row in `outbox_events` for that order id |

### TC-303 An invalid order publishes nothing

| # | Step | Expected result |
|---|---|---|
| 1 | Consume `orders.placed`; place an invalid order (as TC-204), then a valid one | Only the valid order's event appears |

### TC-304 A successful payment marks the order PAID

| # | Step | Expected result |
|---|---|---|
| 1 | Place an order; send `{"eventId":"<new uuid>","orderId":<id>,"result":"SUCCEEDED"}` to `payments.completed` | The order becomes `PAID`; metric `orders.payment.events{outcome="paid"}` + 1 |

### TC-305 A failed payment cancels the order

| # | Step | Expected result |
|---|---|---|
| 1 | Same with `"result":"FAILED"` | The order becomes `CANCELLED` |

### TC-306 A redelivered event is applied only once

| # | Step | Expected result |
|---|---|---|
| 1 | Orders A and B; send event E (FAILED) for A | A `CANCELLED` |
| 2 | Send a copy with the **same** eventId E but for B, SUCCEEDED; then a new event (FAILED) for B | B ends `CANCELLED` (only the new event applied); the duplicate metric + 1; `processed_events` has E once |

### TC-307 Broken JSON goes to the dead-letter topic and does not block later events

| # | Step | Expected result |
|---|---|---|
| 1 | Consume `payments.completed.DLT`; send `{"eventId": "not json at all` for an order | It appears on the DLT at once; header `kafka_dlt-exception-cause-fqcn` names a Jackson exception |
| 2 | Send a valid SUCCEEDED event for the same order | The order becomes `PAID` (the partition is not blocked) |

### TC-308 An event without required fields goes to the dead-letter topic

| # | Step | Expected result |
|---|---|---|
| 1 | Send `{"eventId":"<uuid>","result":"SUCCEEDED"}` (no orderId) | On the DLT; header `kafka_dlt-exception-message` mentions `orderId` |

### TC-309 An event for an unknown order is retried, then dead-lettered

| # | Step | Expected result |
|---|---|---|
| 1 | Send a SUCCEEDED event for order `987654321` (does not exist) | Retried (delivery-failed metric rises), then on the DLT with "No order 987654321"; dead-lettered metric + 1 |
| 2 | `SELECT count(*) FROM processed_events WHERE event_id = '<uuid>'` | `0` (each attempt rolled back) |

### TC-310 A dead-lettered payment can be replayed once its order exists

| # | Step | Expected result |
|---|---|---|
| 1 | Send a SUCCEEDED event for an order that does not exist yet | Dead-lettered ("No order ...") |
| 2 | Insert that order (status PLACED); copy the DLT record back to `payments.completed` (see `docs/dead-letter-replay.md`) | The order becomes `PAID`; the event id is in `processed_events` once |

### TC-311 After a relay crash, a restarted instance republishes the event

Needs two service instances and a way to kill the first right after Kafka acknowledged (the test
injects the crash).

| # | Step | Expected result |
|---|---|---|
| 1 | Instance 1 publishes an order's event, then dies before marking it published | The outbox row stays claimed, `published_at` empty |
| 2 | Start instance 2; wait past the claim lease (30 s by default) | Instance 2 publishes the event again with the **same** eventId; the row is marked published |

### TC-312 The consumer-lag gauge counts waiting payments and returns to 0

| # | Step | Expected result |
|---|---|---|
| 1 | Stop the payment listener (or the consumer); send 3 payment events | `orders_payment_consumer_lag` on `/actuator/prometheus` shows `3` |
| 2 | Start it again | The orders are updated; the gauge returns to `0` |

## Database (`DatabaseMigrationTest`)

### TC-401 All database migrations are applied

| # | Step | Expected result |
|---|---|---|
| 1 | `SELECT version, success FROM flyway_schema_history ORDER BY installed_rank` | Versions 1, 2, 3, 4, all successful |

### TC-402 The database refuses a quantity outside 1-10

| # | Step | Expected result |
|---|---|---|
| 1 | `INSERT INTO orders (customer, sku, quantity, status, created_at) VALUES ('x','y',0,'PLACED',now())` | Check-constraint violation |
| 2 | Same with quantity `11` | Check-constraint violation |

### TC-403 The database refuses an unknown status

| # | Step | Expected result |
|---|---|---|
| 1 | Insert an order with status `SHIPPED` | Check-constraint violation |

### TC-404 A processed event id can be stored only once

| # | Step | Expected result |
|---|---|---|
| 1 | `INSERT INTO processed_events (event_id) VALUES ('6f1c2c1e-0000-4000-8000-000000000001') ON CONFLICT DO NOTHING` twice | First: 1 row; second: 0 rows |

### TC-405 Outbox claims are exclusive, recoverable and ordered per order

| # | Step | Expected result |
|---|---|---|
| 1 | Two pending events for one order and one for another; two relays claim at the same time | No event is claimed by both; the two events of one order are claimed in their order |
| 2 | Let a claim's lease expire | Another relay can claim it; the old owner can no longer mark it published |

### TC-406 Outbox retention deletes only old published rows

| # | Step | Expected result |
|---|---|---|
| 1 | Outbox rows: published 31 days ago, published 10 days ago, pending since 60 days ago; run the cleanup (retention 30 days) | Only the old published row is deleted; the recent one and the old **pending** one stay |

## Operations and performance (`ObservabilityEndpointTest`, `PaymentConsumerPerformanceTest`)

### TC-501 Health endpoint reports the application status

| # | Step | Expected result |
|---|---|---|
| 1 | `GET /actuator/health` | `200`, `{"status":"UP"}` |

### TC-502 Prometheus endpoint exports metrics

| # | Step | Expected result |
|---|---|---|
| 1 | `GET /actuator/prometheus` | `200`, `text/plain` exposition format: `jvm_memory_used_bytes`, and the service's own `orders_outbox_pending`, `orders_outbox_oldest_pending_seconds`, `orders_outbox_published_total`, `orders_outbox_publish_failures_total`, `orders_outbox_cleaned_total`, `orders_payment_consumer_lag` |

### TC-503 The environment endpoint is not exposed

| # | Step | Expected result |
|---|---|---|
| 1 | `GET /actuator/env` | `404` (configuration and bean inventory are not available over HTTP) |

### TC-504 One instance processes a payment backlog above the floor

Run: `mvnw verify -Pperformance`.

| # | Step | Expected result |
|---|---|---|
| 1 | Create 6,000 orders; send one SUCCEEDED payment for each; time until all are PAID | More than 50 events per second (measured: ~190/s on a laptop, ~830/s on a CI runner) |

### TC-505 Rebalances during processing lose nothing and apply nothing twice

| # | Step | Expected result |
|---|---|---|
| 1 | Send 6,000 payments at 300 per second; start a second instance during the stream and stop it after two thirds | Every order PAID exactly once (6,000 processed events, no duplicates); no pause in progress longer than 30 s |
