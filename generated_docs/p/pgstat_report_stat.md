# pgstat_report_stat

## Location
[src/backend/utils/activity/pgstat.c:579-692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L579-L692)

## Overview
The primary function responsible for flushing pending statistics updates from backend processes to shared memory, controlling the timing and frequency of statistics reporting in PostgreSQL.

## Definition

```c
long
pgstat_report_stat(bool force)
```
## Detailed Description
This function serves as the central coordinator for statistics reporting in PostgreSQL. It manages the periodic flushing of various types of statistics (database, relation, function, I/O, WAL, and SLRU stats) from backend processes to shared memory where they can be accessed by other processes.

The function implements intelligent timing controls to balance performance with data freshness:
- Without force=true, it enforces a minimum interval (PGSTAT_MIN_INTERVAL = 1000ms) between flushes to avoid excessive overhead
- It enforces a maximum interval (PGSTAT_MAX_INTERVAL = 60000ms) to ensure stats don't become too stale
- When forced flushing is disabled, it uses non-blocking lock acquisition to prevent performance impact
- Returns a suggested idle timeout (PGSTAT_IDLE_INTERVAL = 10000ms) when pending updates remain

The function maintains static variables to track timing state across calls and coordinates with the global pgStatForceNextFlush flag.

## Parameters / Member Variables
- `force`: When true, forces immediate flushing regardless of timing intervals and uses blocking lock acquisition
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_assert_is_up](pgstat_assert_is_up.md)
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [pgstat_have_pending_wal](pgstat_have_pending_wal.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [GetCurrentTransactionStopTimestamp](../G/GetCurrentTransactionStopTimestamp.md)
  - [TimestampDifferenceExceeds](../T/TimestampDifferenceExceeds.md)
  - [pgstat_update_dbstats](pgstat_update_dbstats.md)
  - [pgstat_flush_pending_entries](pgstat_flush_pending_entries.md)
  - [pgstat_flush_io](pgstat_flush_io.md)
  - [pgstat_flush_wal](pgstat_flush_wal.md)
  - [pgstat_slru_flush](pgstat_slru_flush.md)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main query processing loop)
  - [ProcessInterrupts](../P/ProcessInterrupts.md) (interrupt handling)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (logical replication)
  - [pgstat_shutdown_hook](pgstat_shutdown_hook.md) (shutdown processing)
  - [worker_spi_main](../w/worker_spi_main.md) (background worker processes)

## Notes and Other Information
- Must only be called outside of transactions (enforced by assertion)
- Uses transaction stop time as an approximation of current time for performance when not forced
- Implements partial flush detection - if any category of stats cannot be flushed due to lock contention, returns timeout for retry
- The function is critical for PostgreSQL's statistics collection system, balancing real-time visibility with system performance
- Static variables (pending_since, last_flush) maintain state across function calls within the same backend process

## Simplified Source

```c
// Simplified version of pgstat_report_stat
long pgstat_report_stat(bool force) {
    static TimestampTz pending_since = 0;
    static TimestampTz last_flush = 0;
    bool partial_flush;
    TimestampTz now;

    // Basic validation
    pgstat_assert_is_up();
    Assert(!IsTransactionOrTransactionBlock());

    // Handle forced flush flag
    if (pgStatForceNextFlush) {
        force = true;
        pgStatForceNextFlush = false;
    }

    // Early exit if nothing to report
    if (no_pending_stats()) {
        return 0;
    }

    // Determine current time based on force flag
    if (force) {
        now = GetCurrentTimestamp();
    } else {
        now = GetCurrentTransactionStopTimestamp();

        // Check if we've been pending too long - force flush
        if (pending_too_long(pending_since, now)) {
            force = true;
        }
        // Check if we're flushing too frequently - defer
        else if (flushing_too_frequently(last_flush, now)) {
            if (pending_since == 0)
                pending_since = now;
            return PGSTAT_IDLE_INTERVAL;
        }
    }

    // Update database-level stats
    pgstat_update_dbstats(now);

    // Flush all types of pending statistics
    // Use non-blocking locks unless forced
    bool nowait = !force;
    partial_flush = false;

    partial_flush |= pgstat_flush_pending_entries(nowait);
    partial_flush |= pgstat_flush_io(nowait);
    partial_flush |= pgstat_flush_wal(nowait);
    partial_flush |= pgstat_slru_flush(nowait);

    last_flush = now;

    // Handle partial flush (some stats couldn't be flushed due to lock contention)
    if (partial_flush) {
        if (pending_since == 0)
            pending_since = now;
        return PGSTAT_IDLE_INTERVAL;  // Suggest retry timeout
    }

    // All stats successfully flushed
    pending_since = 0;
    return 0;  // No timeout needed
}
```

Key simplifications made:
- Abstracted complex timing checks into helper function concepts (`pending_too_long`, `flushing_too_frequently`, `no_pending_stats`)
- Consolidated the detailed condition checks for readability
- Simplified the timestamp logic flow
- Removed detailed comments about implementation specifics
- Focused on the main execution path and core algorithm
- Preserved the essential timing control mechanism and flush coordination logic