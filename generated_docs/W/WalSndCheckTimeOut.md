# WalSndCheckTimeOut

## Location
[src/backend/replication/walsender.c:2759-2785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L2759-L2785)

## Overview
Monitors client responsiveness and terminates the WAL sender process if no replies are received within the configured timeout period.

## Definition
```c
static void WalSndCheckTimeOut(void)
```

## Detailed Description
This function implements the timeout detection mechanism for WAL sender processes. It checks whether the client has responded within the `wal_sender_timeout` period and initiates a graceful shutdown if the timeout is exceeded. The function uses `last_processing` as the reference point to avoid counting server-side stalls against the client, ensuring fair timeout detection.

The function includes sophisticated timing logic to handle edge cases where server-side stalls might cause keepalive messages to be sent later than expected. When a timeout is detected, the process terminates with a communication error without sending an error message to the standby, as timeout typically indicates a communication problem.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - TimestampTzPlusMilliseconds
  - COMMERROR
  - [WalSndShutdown](WalSndShutdown.md)
- Called from:
  - [ProcessPendingWrites](../P/ProcessPendingWrites.md) (src/backend/replication/walsender.c:1628)
  - [WalSndWaitForWal](WalSndWaitForWal.md) (src/backend/replication/walsender.c:1947)
  - [WalSndLoop](WalSndLoop.md) (src/backend/replication/walsender.c:2874)

## Notes and Other Information
- Returns early if `last_reply_timestamp` is not set (≤ 0), indicating no timeout enforcement needed
- Uses `last_processing` rather than current time to avoid penalizing clients for server-side delays
- Does not send error messages to standby when timeout occurs, as this typically indicates communication issues
- Default configuration has clients send messages every 10 seconds (`standby_message_timeout = wal_sender_timeout/6`)
- Timeout expiration could be optimized by recognizing expiration at `wal_sender_timeout/2` after keepalive transmission
- Critical for maintaining replication connection health and preventing resource leaks from disconnected clients

## Simplified Source

```c
// Simplified version of WalSndCheckTimeOut
static void WalSndCheckTimeOut(void) {
    TimestampTz timeout;

    // Skip timeout check if no client communication has been established
    if (last_reply_timestamp <= 0)
        return;

    // Calculate when the timeout should expire based on last client reply
    timeout = TimestampTzPlusMilliseconds(last_reply_timestamp, wal_sender_timeout);

    // Check if we've exceeded the timeout period
    if (wal_sender_timeout > 0 && last_processing >= timeout) {
        // Log timeout error (communication problem suspected)
        ereport(COMMERROR,
                (errmsg("terminating walsender process due to replication timeout")));

        // Gracefully shutdown the WAL sender process
        WalSndShutdown();
    }
}
```

Key simplifications made:
- Removed detailed comment block explaining timing edge cases for clarity
- Simplified variable declarations and logic flow
- Condensed timeout calculation logic into clear steps
- Preserved essential error handling and shutdown mechanism
- Maintained the core algorithm: check timestamp → calculate timeout → shutdown if exceeded