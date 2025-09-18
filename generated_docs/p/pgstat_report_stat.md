# pgstat_report_stat

## Location
src/backend/utils/activity/pgstat.c: 579 - 692

## Overview
The primary function responsible for flushing pending statistics updates from backend processes to shared memory, controlling the timing and frequency of statistics reporting in PostgreSQL.

## Definition


## Detailed Description
This function serves as the central coordinator for statistics reporting in PostgreSQL. It manages the periodic flushing of various types of statistics (database, relation, function, I/O, WAL, and SLRU stats) from backend processes to shared memory where they can be accessed by other processes.

The function implements intelligent timing controls to balance performance with data freshness:
- Without force=true, it enforces a minimum interval (PGSTAT_MIN_INTERVAL = 1000ms) between flushes to avoid excessive overhead
- It enforces a maximum interval (PGSTAT_MAX_INTERVAL = 60000ms) to ensure stats don't become too stale
- When forced flushing is disabled, it uses non-blocking lock acquisition to prevent performance impact
- Returns a suggested idle timeout (PGSTAT_IDLE_INTERVAL = 10000ms) when pending updates remain

The function maintains static variables to track timing state across calls and coordinates with the global pgStatForceNextFlush flag.

## Parameters / Member Variables
- : When true, forces immediate flushing regardless of timing intervals and uses blocking lock acquisition

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_assert_is_up
  - IsTransactionOrTransactionBlock
  - dlist_is_empty
  - pgstat_have_pending_wal
  - GetCurrentTimestamp
  - GetCurrentTransactionStopTimestamp
  - TimestampDifferenceExceeds
  - pgstat_update_dbstats
  - pgstat_flush_pending_entries
  - pgstat_flush_io
  - pgstat_flush_wal
  - pgstat_slru_flush
- Called from (representative examples):
  - PostgresMain (main query processing loop)
  - ProcessInterrupts (interrupt handling)
  - LogicalRepApplyLoop (logical replication)
  - pgstat_shutdown_hook (shutdown processing)
  - worker_spi_main (background worker processes)

## Notes and Other Information
- Must only be called outside of transactions (enforced by assertion)
- Uses transaction stop time as an approximation of current time for performance when not forced
- Implements partial flush detection - if any category of stats cannot be flushed due to lock contention, returns timeout for retry
- The function is critical for PostgreSQL's statistics collection system, balancing real-time visibility with system performance
- Static variables (pending_since, last_flush) maintain state across function calls within the same backend process