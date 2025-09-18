# WalSndComputeSleeptime

## Location
src/backend/replication/walsender.c: 2715 - 2758

## Overview
Computes the optimal sleep duration for WAL sender send/receive loops, considering timeout settings and keepalive requirements.

## Definition
```c
static long WalSndComputeSleeptime(TimestampTz now)
```

## Detailed Description
This function calculates how long the WAL sender should sleep during send/receive operations. It implements intelligent timeout management by considering the `wal_sender_timeout` configuration and the need to send periodic keepalive messages. The function ensures that the WAL sender wakes up in time to either send keepalives or detect timeout conditions.

The base sleep time is set to 10 seconds, but this is adjusted based on timeout settings. When `wal_sender_timeout` is enabled and there has been a previous reply, the function calculates wake-up times to ensure timely keepalive transmission and timeout detection.

## Parameters / Member Variables
- `now`: Current timestamp used to calculate relative sleep duration

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTzPlusMilliseconds
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
- Called from:
  - [ProcessPendingWrites](../P/ProcessPendingWrites.md) (src/backend/replication/walsender.c:1636)
  - [WalSndWaitForWal](WalSndWaitForWal.md) (src/backend/replication/walsender.c:1960)
  - [WalSndLoop](WalSndLoop.md) (src/backend/replication/walsender.c:2906)

## Notes and Other Information
- Returns a default sleep time of 10 seconds when timeout management is disabled
- When timeout is enabled, calculates wake-up time based on `last_reply_timestamp` and `wal_sender_timeout`
- Implements half-timeout keepalive logic: sends keepalives when half the timeout period has elapsed without response
- Uses `waiting_for_ping_response` flag to determine appropriate wake-up timing
- Critical for maintaining responsive replication connections and preventing unnecessary disconnections