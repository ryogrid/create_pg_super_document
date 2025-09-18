# WalSndWakeupProcessRequests

## Location
src/include/replication/walsender.h: 66 - 76

## Overview
A static inline function that conditionally wakes up WAL sender processes when there is replication work to be done, checking a global flag to avoid unnecessary wake-ups.

## Definition
```c
static inline void
WalSndWakeupProcessRequests(bool physical, bool logical)
```

## Detailed Description
WalSndWakeupProcessRequests is a lightweight wrapper function that implements a conditional wake-up mechanism for WAL (Write-Ahead Log) sender processes. It uses a global flag `wake_wal_senders` to track whether wake-up requests are pending, and only performs the actual wake-up operation when necessary. This optimization helps reduce unnecessary overhead by avoiding redundant wake-up calls when WAL senders are already active or when no senders are configured.

The function first checks if `wake_wal_senders` is true, indicating that a wake-up is needed. If so, it resets the flag to false and then calls the actual wake-up function `WalSndWakeup` only if `max_wal_senders` is greater than 0 (meaning WAL senders are configured).

## Parameters / Member Variables
- `physical`: Boolean flag indicating whether to wake up physical replication WAL senders
- `logical`: Boolean flag indicating whether to wake up logical replication WAL senders

## Dependencies
- Functions called/Symbols referenced:
  - [WalSndWakeup](WalSndWakeup.md)
- Global variables accessed:
  - wake_wal_senders (flag indicating wake-up is needed)
  - max_wal_senders (maximum number of WAL sender processes)
- Called from (representative examples):
  - [XLogFlush](../X/XLogFlush.md)
  - [XLogBackgroundFlush](../X/XLogBackgroundFlush.md)

## Notes and Other Information
- This is a static inline function defined in the header file for performance optimization
- The function implements a simple state machine using the `wake_wal_senders` flag to avoid redundant wake-up operations
- It's part of PostgreSQL's streaming replication infrastructure
- The function is typically called after WAL records are flushed to disk, ensuring that standby servers can receive updates promptly
- The conditional check on `max_wal_senders` prevents unnecessary work when no WAL senders are configured