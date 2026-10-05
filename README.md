# NSE MITCH market data client

Requires Java 21, Spring Boot 3, Spring Kafka.

Replay and snapshot requests arrive as JSON on Kafka. Each request opens its own connection to the gateway, logs in,
sends the request, consumes until the completion marker and disconnects.

| Topic | Payload | Gateway port |
|---|---|---|
| `MARKET_REPLAY_REQUEST` | `{"count":4,"marketDataGroup":4,"startSequence":4,"requestID":305}` | `port` |
| `SNAPSHOT_REQUEST` | `{"requestID":5001,"instrumentId":0,"marketDataGroup":4,"snapshotType":1}` | `snapshot-port` |

Copy `src/main/java/com/nse/testtcpclient/**` into your project, delete the old `MarketDataClient`, `UnitHeader` and
`ReplayRequestDto`, and add the `nse.mitch.replay` block from `src/main/resources/application.yml` (credentials via env vars).

## Layout

| Class | Role |
|---|---|
| `kafka/MarketDataRequestListener` | `@KafkaListener`s for both topics; one request at a time per topic |
| `kafka/MarketDataKafkaConfiguration` | JSON -> DTO conversion (`StringJsonMessageConverter`) |
| `request/ReplayRequestDto`, `request/SnapshotRequestDto` | Topic payloads |
| `MarketDataClient` | `replay(dto)` / `snapshot(dto)`: connection, login, request, completion, retries, symbol cache |
| `protocol/FrameReader` | Reads length-prefixed frames, validates length, handles partial reads |
| `protocol/MitchEncoder` | Builds login / replay / snapshot requests |
| `protocol/MitchDecoder` | Pure frame -> `List<MitchMessage>` decoder; per-message isolation |
| `protocol/MitchMessage` | Sealed interface of decoded records (`AddOrder`, `SymbolDirectory`, ...) |
| `config/MitchProperties` | `nse.mitch.replay.*` configuration, validated at startup |

## Consuming data

```java
@EventListener
void onMessage(MarketDataMessageEvent event) {   // type, requestId, message
    if (event.message() instanceof MitchMessage.AddOrder order) { ... }
}

@EventListener
void onResult(MarketDataRequestResult result) { ... }   // COMPLETED | INCOMPLETE | FAILED
```

## Behaviour

* Connection failures before any market data arrives are retried (`max-connect-attempts`, `retry-backoff`). Rejected
  logins/requests and failures after data has arrived are not retried, to avoid duplicate events.
* `completion-timeout` is an idle timeout: the request ends as `INCOMPLETE` after that long without data and without a
  completion marker.
* `requestID` is not sent in replay requests (the wire format has no field for it); it tags events and results.

## Wire format

Unit header (little-endian): `length:u16 | messageCount:u8 | marketDataGroup:u8 | sequence:u32`, same as `UnitHeader`.

## Verify against the venue spec

* `nse.mitch.replay.inner-length-includes-length-field` (default `false`). Outbound requests count the 2-byte length
  field; if inbound bundles do too, set this to `true`.
* Add Order price is decoded as a 4-byte int with 4 implied decimals (`BigDecimal`).

## Build

```
mvn test
```
