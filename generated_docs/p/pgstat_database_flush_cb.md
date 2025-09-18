# pgstat_database_flush_cb

## Location
src/backend/utils/activity/pgstat_database.c: 375 - 385

## Overview
A callback function that flushes pending database statistics from local pending storage to shared memory, accumulating various database-level performance metrics.

## Definition


## Detailed Description
This function serves as a flush callback specifically for database-level statistics in PostgreSQL's statistics collection system. It transfers accumulated database statistics from the local pending entry to the shared statistics entry, where they can be accessed by other processes and the statistics views like .

The function uses a lock-based approach to ensure thread safety when updating shared statistics. It accumulates various database performance counters including transaction counts, block I/O statistics, tuple operations, session information, and conflict statistics. After successfully flushing the data, it clears the pending entry to prepare for the next collection cycle.

The function is registered as a callback in the PostgreSQL statistics infrastructure and is automatically called when database statistics need to be flushed to shared memory.

## Parameters / Member Variables
- : Pointer to the statistics entry reference containing both pending and shared statistics structures
- : Boolean flag indicating whether to wait for lock acquisition. If true and lock cannot be immediately acquired, the function returns false without flushing

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_lock_entry
  - pgstat_unlock_entry
  - PgStat_EntryRef
  - [PgStatShared_Database](../P/PgStatShared_Database.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md)
- Called from (representative examples):
  - Statistics infrastructure via PGSTAT_KIND_DATABASE configuration in pgstat.c

## Notes and Other Information
- The function accumulates numerous database statistics including transaction commits/rollbacks, block reads/hits, tuple operations, session timing, deadlocks, temporary file usage, and various types of conflicts
- Some statistics like autovacuum time and checksum failures are reported immediately and should be zero in pending entries (enforced by assertions)
- The function uses a macro PGSTAT_ACCUM_DBCOUNT to accumulate each statistic type, adding pending values to shared values
- Returns true on successful flush, false if the lock could not be acquired when nowait=true
- After flushing, the pending entry is cleared with memset to prepare for next statistics collection cycle
- This callback is part of PostgreSQL's pluggable statistics architecture, registered for PGSTAT_KIND_DATABASE statistics