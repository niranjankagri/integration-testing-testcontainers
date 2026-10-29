# Replaying dead-lettered payment events

A payment event that cannot be processed ends up in `payments.completed.DLT`: broken JSON and events with missing
fields at once, an event for an unknown order after two retries (see `KafkaConfig`). This page is the procedure to
look at those events and, when the cause is fixed, send them through again.

The commands use the Kafka command-line tools (`kafka-console-consumer.sh`, `kafka-console-producer.sh`, shipped with
Kafka and in the `apache/kafka` image). They were checked against `apache/kafka:4.1.1`. Add your cluster's
connection and security options to `--bootstrap-server`.

## 1. Notice it

- The `PaymentDeadLettered` alert fires (Prometheus, local stack: http://localhost:9090/alerts), and the Grafana panel
  "Dead-lettered events (last hour)" shows it. The counter behind both is
  `orders_payment_events_total{outcome="dead_lettered"}`.
- `PaymentRecoveryFailed` is more urgent: publishing to the dead-letter topic itself failed, so the event may be lost
  and has to be recovered from the producer side.

## 2. Look at the dead-lettered records

```sh
kafka-console-consumer.sh --bootstrap-server <broker:port> --topic payments.completed.DLT --from-beginning \
  --timeout-ms 10000 \
  --property print.partition=true --property print.offset=true \
  --property print.headers=true --property print.key=true
```

Each line shows partition, offset, headers, key (the order id) and the original event, for example:

```text
Partition:1  Offset:0  kafka_dlt-exception-message:No order 9876543210,kafka_dlt-original-topic:payments.completed,...  9876543210  {"eventId":"0b9a2f1e-...","orderId":9876543210,"result":"SUCCEEDED"}
```

`kafka_dlt-exception-message` says why the event failed. (The original partition and offset headers are binary
numbers and print as unreadable characters; they are not needed for a replay.) To find the events of one order, filter
the output by its id.

## 3. Decide per event

| Failure | Replay? |
|---|---|
| `No order <id>`: the order did not exist yet | yes, once the order exists (check with the `order(id)` query) |
| A bug in the listener, now fixed and deployed | yes |
| Broken JSON or missing fields | **no**: it fails the same way again. Fix it at the producer, which sends a corrected event with a new event id |

## 4. Replay one record

Replay by partition and offset from step 2. This copies exactly that record to the input topic with its **key**, so it
lands in the same partition as the order's other events and keeps their order. The dead-letter headers are not copied.

```sh
kafka-console-consumer.sh --bootstrap-server <broker:port> --topic payments.completed.DLT \
    --partition 1 --offset 0 --max-messages 1 \
    --property print.key=true --property key.separator='|' \
  | kafka-console-producer.sh --bootstrap-server <broker:port> --topic payments.completed \
    --property parse.key=true --property key.separator='|'
```

`|` separates key and value because neither contains it. The record stays in the dead-letter topic: it is an
append-only log, so keep a note of the offsets you replayed.

## 5. Check the result

- The order has the expected status (`PAID` or `CANCELLED`) and the event id is in `processed_events`.
- No new record with that event id appeared in the dead-letter topic.

Replaying is safe to repeat: the listener records each event id in `processed_events` in the same transaction as the
order change, so a second replay of the same event is skipped and counted as `duplicate`. An event for an order that is
no longer `PLACED` is ignored (counted as `ignored`), so a stale payment cannot overwrite a newer decision.

## How this is tested

`PaymentEventsTest.aDeadLetteredPaymentCanBeReplayedWhenItsOrderAppears` runs this procedure in code: a payment for an
unknown order is dead-lettered, the order is created, the dead-lettered value is sent to `payments.completed` with its
key, and the order becomes `PAID` with the event id recorded once.
