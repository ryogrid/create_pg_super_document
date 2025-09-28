# WalSndWaitForWal

## Location
[src/backend/replication/walsender.c:1822-1991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1822-L1991)

## Overview
WalSndWaitForWal is the core waiting mechanism in WAL senders that ensures WAL records are flushed to disk before being safely sent to clients, and coordinates with standby servers for logical failover slots.

## Definition
static XLogRecPtr WalSndWaitForWal(XLogRecPtr loc)

## Detailed Description
WalSndWaitForWal implements a sophisticated waiting loop that handles multiple coordination requirements in PostgreSQL's replication system:

1. **WAL Flush Coordination**: The primary function ensures that WAL records up to the specified location are flushed to disk before they can be safely transmitted to clients.

2. **Logical Failover Support**: For logical failover slots, it additionally waits for all specified streaming replication standby servers to confirm receipt of WAL up to the recently flushed position (RecentFlushPtr).

3. **Fast Path Optimization**: Includes an optimized path that avoids spinlock acquisition when sufficient WAL is already available and standby confirmations are up-to-date.

4. **Graceful Shutdown Handling**: The function handles shutdown signals appropriately, triggering background WAL flushing and ensuring clean termination after standby synchronization.

5. **Client Communication**: Maintains communication with clients through keepalive messages and processes incoming replies during the wait period.

6. **Resource Management**: Includes timeout handling, statistics reporting, and proper latch management for efficient waiting.

The function returns the end LSN of flushed WAL, which is normally >= the requested location, but may return early if shutdown is detected.

## Parameters / Member Variables
- `loc`: The target WAL LSN (Log Sequence Number) that must be flushed to disk before the function can return

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - [NeedToWaitForWal](../N/NeedToWaitForWal.md)
  - [ResetLatch](../R/ResetLatch.md), SetLatch
  - [ProcessRepliesIfAny](../P/ProcessRepliesIfAny.md)
  - [XLogBackgroundFlush](../X/XLogBackgroundFlush.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md), GetXLogReplayRecPtr
  - [NeedToWaitForStandbys](../N/NeedToWaitForStandbys.md)
  - [WalSndKeepalive](WalSndKeepalive.md), WalSndKeepaliveIfNecessary
  - [WalSndShutdown](WalSndShutdown.md), WalSndCheckTimeOut
  - [WalSndComputeSleeptime](WalSndComputeSleeptime.md), WalSndWait
  - pq_flush_if_writable, pq_is_send_pending
  - [SyncRepInitConfig](../S/SyncRepInitConfig.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md), TimestampDifferenceExceeds
  - [pgstat_flush_io](../p/pgstat_flush_io.md)
- Called from (representative examples):
  - [logical_read_xlog_page](../l/logical_read_xlog_page.md) (at src/backend/replication/walsender.c:1069)

## Notes and Other Information
- This is a static function within walsender.c, serving as a critical internal component of the WAL sender infrastructure
- The function implements a complex state machine that handles multiple concurrent requirements: WAL flushing, standby coordination, client communication, and graceful shutdown
- Uses a cached RecentFlushPtr value to avoid frequent system calls for flush position queries
- The wait event mechanism provides detailed monitoring capabilities for different wait conditions
- Includes optimization for scenarios where the WAL sender is far behind, using fast-path checks to avoid unnecessary work
- Critical for maintaining data consistency in logical replication with failover capabilities
- The function's ability to return early on shutdown makes it essential for responsive system shutdown procedures

## Simplified Source

```c
// Simplified version of WalSndWaitForWal
static XLogRecPtr WalSndWaitForWal(XLogRecPtr loc) {
    int wakeEvents;
    uint32 wait_event = 0;
    static XLogRecPtr RecentFlushPtr = InvalidXLogRecPtr;
    TimestampTz last_flush = 0;

    // Fast path: if we already have enough WAL and standbys are caught up
    if (!XLogRecPtrIsInvalid(RecentFlushPtr) &&
        !NeedToWaitForWal(loc, RecentFlushPtr, &wait_event))
        return RecentFlushPtr;

    // Main waiting loop
    for (;;) {
        bool wait_for_standby_at_stop = false;
        long sleeptime;
        TimestampTz now;

        // Clear any pending wakeups and check for interrupts
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        // Handle configuration reloads
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();
        }

        // Process client replies
        ProcessRepliesIfAny();

        // Trigger WAL flush if shutting down
        if (got_STOPPING)
            XLogBackgroundFlush();

        // Update flush position (unless waiting for standby confirmation)
        if (wait_event != WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION) {
            if (!RecoveryInProgress())
                RecentFlushPtr = GetFlushRecPtr(NULL);
            else
                RecentFlushPtr = GetXLogReplayRecPtr(NULL);
        }

        // Check if we can stop during shutdown
        if (got_STOPPING) {
            if (NeedToWaitForStandbys(RecentFlushPtr, &wait_event))
                wait_for_standby_at_stop = true;
            else
                break;
        }

        // Send keepalive if needed
        if (MyWalSnd->flush < sentPtr && MyWalSnd->write < sentPtr && !waiting_for_ping_response)
            WalSndKeepalive(false, InvalidXLogRecPtr);

        // Exit if caught up and no standby wait needed
        if (!wait_for_standby_at_stop && !NeedToWaitForWal(loc, RecentFlushPtr, &wait_event))
            break;

        // Mark as caught up
        WalSndCaughtUp = true;

        // Flush pending output
        if (pq_flush_if_writable() != 0)
            WalSndShutdown();

        // Exit if streaming is done
        if (streamingDoneReceiving && streamingDoneSending && !pq_is_send_pending())
            break;

        // Check for timeout and send keepalives
        WalSndCheckTimeOut();
        WalSndKeepaliveIfNecessary();

        // Calculate sleep time and wait
        now = GetCurrentTimestamp();
        sleeptime = WalSndComputeSleeptime(now);
        wakeEvents = WL_SOCKET_READABLE;
        if (pq_is_send_pending())
            wakeEvents |= WL_SOCKET_WRITEABLE;

        // Flush IO statistics periodically
        if (TimestampDifferenceExceeds(last_flush, now, WALSENDER_STATS_FLUSH_INTERVAL)) {
            pgstat_flush_io(false);
            last_flush = now;
        }

        WalSndWait(wakeEvents, sleeptime, wait_event);
    }

    // Reactivate latch and return flush position
    SetLatch(MyLatch);
    return RecentFlushPtr;
}
```

Key simplifications made:
- Maintained core waiting loop structure and essential logic
- Preserved WAL flush coordination and standby confirmation handling
- Kept client communication and shutdown handling intact
- Simplified complex condition checks while preserving functionality