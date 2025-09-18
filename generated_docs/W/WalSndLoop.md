# WalSndLoop

## Location
src/backend/replication/walsender.c: 2786 - 2926

## Overview
Main control loop for WAL sender processes that manages streaming WAL data to replicas via Copy protocol messages.

## Definition
```c
static void WalSndLoop(WalSndSendDataCallback send_data)
```

## Detailed Description
This function implements the core streaming loop for WAL sender processes. It coordinates all aspects of WAL streaming including data transmission, client communication, timeout monitoring, keepalive management, and state transitions. The loop continues until replication ends or the client requests termination.

The function handles both physical and logical replication through a callback mechanism, manages the transition from catchup to streaming state, processes configuration reloads, and implements proper cleanup on shutdown signals. It includes sophisticated I/O management to avoid blocking while ensuring timely data delivery.

## Parameters / Member Variables
- `send_data`: Callback function pointer for sending WAL data (physical or logical replication specific)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [ResetLatch](../R/ResetLatch.md)
  - ProcessConfigFile (with PGC_SIGHUP)
  - [SyncRepInitConfig](../S/SyncRepInitConfig.md)
  - [ProcessRepliesIfAny](../P/ProcessRepliesIfAny.md)
  - pq_is_send_pending
  - pq_flush_if_writable
  - [WalSndShutdown](WalSndShutdown.md)
  - [WalSndSetState](WalSndSetState.md)
  - [WalSndDone](WalSndDone.md)
  - [WalSndCheckTimeOut](WalSndCheckTimeOut.md)
  - [WalSndKeepaliveIfNecessary](WalSndKeepaliveIfNecessary.md)
  - [WalSndComputeSleeptime](WalSndComputeSleeptime.md)
  - [WalSndWait](WalSndWait.md)
  - [pgstat_flush_io](../p/pgstat_flush_io.md)
- Called from:
  - [StartReplication](../S/StartReplication.md) (src/backend/replication/walsender.c:988)
  - [StartLogicalReplication](../S/StartLogicalReplication.md) (src/backend/replication/walsender.c:1525)

## Notes and Other Information
- Initializes `last_reply_timestamp` to enable timeout processing
- Implements state machine with WALSNDSTATE_CATCHUP and WALSNDSTATE_STREAMING states
- Handles graceful shutdown on SIGUSR2 signal through `got_SIGUSR2` flag
- Uses different blocking strategies for physical vs logical replication (XLogSendLogical check)
- Implements I/O statistics reporting with WALSENDER_STATS_FLUSH_INTERVAL timing
- Manages wake events (WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE) for efficient waiting
- Critical transition point where data loss risk ends when moving from catchup to streaming state
- Processes configuration reloads dynamically without restart
- Coordinates with synchronous replication through state changes