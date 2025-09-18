# pgstat_relation_flush_cb

## Location
src/backend/utils/activity/pgstat_relation.c: 802 - 884

## Overview
Flushes pending relation statistics from local backend state to shared memory, transferring accumulated counters to both relation and database statistics entries.

## Definition
```c
bool pgstat_relation_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
```

## Detailed Description
This callback function is responsible for transferring accumulated relation statistics from the local backend's pending statistics to the shared memory statistics structures. The function performs a comprehensive update of various counters including scan statistics, tuple operations (insert/update/delete), buffer access statistics, and live/dead tuple counts.

The function implements optimizations such as ignoring entries with zero counts (like unused planner-opened indexes) and provides non-blocking operation support. It handles special cases like truncated/dropped tables by resetting live/dead counters before applying deltas. After updating relation statistics, it also contributes the same data to database-level aggregate statistics.

The function ensures data consistency through proper locking and implements safeguards like clamping negative values to prevent inconsistent statistics.

## Parameters / Member Variables
- `entry_ref`: Reference to the statistics entry containing both pending and shared data structures
- `nowait`: If true, returns false immediately if the lock cannot be acquired without waiting

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_lock_entry
  - pgstat_unlock_entry
  - [GetCurrentTransactionStopTimestamp](../G/GetCurrentTransactionStopTimestamp.md)
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - PgStat_EntryRef (data structure)
  - PgStat_TableStatus (data structure)
  - [PgStatShared_Relation](../P/PgStatShared_Relation.md) (data structure)
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md) (data structure)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (data structure)
  - PgStat_TableCounts (data structure)
- Called from (representative examples):
  - Statistics hash table management system (SH_DECLARE in pgstat.c)

## Notes and Other Information
- Returns true on successful flush, false if nowait is true and lock cannot be acquired immediately
- Implements zero-count optimization to avoid processing unused index entries
- Handles truncated/dropped relations by resetting counters before applying deltas
- Updates both relation-specific and database-aggregate statistics in a single operation
- Implements safeguards against negative live/dead tuple counts through Max() clamping
- Part of PostgreSQL's statistics collection system that bridges local backend state with shared memory