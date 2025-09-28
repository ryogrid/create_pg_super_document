# WalSndLoop

## Location
[src/backend/replication/walsender.c:2786-2926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L2786-L2926)

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

## Simplified Source

```c
// Simplified version of WalSndLoop
static void WalSndLoop(WalSndSendDataCallback send_data) {
    TimestampTz last_flush = 0;

    // Initialize timing for timeout processing
    last_reply_timestamp = GetCurrentTimestamp();
    waiting_for_ping_response = false;

    // Main streaming loop
    for (;;) {
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        // Handle configuration reloads
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();
        }

        // Process client messages
        ProcessRepliesIfAny();

        // Check for streaming completion
        if (streamingDoneReceiving && streamingDoneSending &&
            !pq_is_send_pending())
            break;

        // Send more data if output buffer is empty
        if (!pq_is_send_pending())
            send_data();
        else
            WalSndCaughtUp = false;

        // Flush output to client
        if (pq_flush_if_writable() != 0)
            WalSndShutdown();

        // Handle state transitions and shutdown
        if (WalSndCaughtUp && !pq_is_send_pending()) {
            // Transition from catchup to streaming state
            if (MyWalSnd->state == WALSNDSTATE_CATCHUP) {
                ereport(DEBUG1, (errmsg_internal("\"%s\" has now caught up with upstream server",
                                                 application_name)));
                WalSndSetState(WALSNDSTATE_STREAMING);
            }

            // Handle shutdown signal
            if (got_SIGUSR2)
                WalSndDone(send_data);
        }

        // Monitor timeouts and send keepalives
        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary();

        // Block/wait for more work
        if ((WalSndCaughtUp && send_data != XLogSendLogical && !streamingDoneSending) ||
            pq_is_send_pending()) {
            long sleeptime;
            int wakeEvents;
            TimestampTz now;

            // Set up wake conditions
            wakeEvents = !streamingDoneReceiving ? WL_SOCKET_READABLE : 0;
            if (pq_is_send_pending())
                wakeEvents |= WL_SOCKET_WRITEABLE;

            // Calculate sleep time and report I/O stats periodically
            now = GetCurrentTimestamp();
            sleeptime = WalSndComputeSleeptime(now);

            if (TimestampDifferenceExceeds(last_flush, now, WALSENDER_STATS_FLUSH_INTERVAL)) {
                pgstat_flush_io(false);
                last_flush = now;
            }

            // Wait for events or timeout
            WalSndWait(wakeEvents, sleeptime, WAIT_EVENT_WAL_SENDER_MAIN);
        }
    }
}
```

Key simplifications made:
- Added clear comments for each major section of the loop
- Grouped related operations together logically
- Simplified conditional logic while preserving essential flow
- Maintained all critical timing, state management, and I/O handling
- Preserved the complex wake event logic for proper blocking behavior