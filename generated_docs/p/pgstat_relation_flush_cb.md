# pgstat_relation_flush_cb

## Location
[src/backend/utils/activity/pgstat_relation.c:802-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L802-L884)

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
  - [pgstat_lock_entry](pgstat_lock_entry.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - [GetCurrentTransactionStopTimestamp](../G/GetCurrentTransactionStopTimestamp.md)
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (data structure)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md) (data structure)
  - [PgStatShared_Relation](../P/PgStatShared_Relation.md) (data structure)
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md) (data structure)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (data structure)
  - [PgStat_TableCounts](../P/PgStat_TableCounts.md) (data structure)
- Called from (representative examples):
  - Statistics hash table management system (SH_DECLARE in pgstat.c)

## Notes and Other Information
- Returns true on successful flush, false if nowait is true and lock cannot be acquired immediately
- Implements zero-count optimization to avoid processing unused index entries
- Handles truncated/dropped relations by resetting counters before applying deltas
- Updates both relation-specific and database-aggregate statistics in a single operation
- Implements safeguards against negative live/dead tuple counts through Max() clamping
- Part of PostgreSQL's statistics collection system that bridges local backend state with shared memory

## Simplified Source

```c
bool
pgstat_relation_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
    static const PgStat_TableCounts all_zeroes;
    Oid dboid;
    PgStat_TableStatus *lstats;     /* pending stats entry  */
    PgStat_StatTabEntry *tabentry;  /* shared stats entry */

    dboid = entry_ref->shared_entry->key.dboid;
    lstats = (PgStat_TableStatus *) entry_ref->pending;

    // Skip entries with no accumulated counts (e.g., unused indexes)
    if (memcmp(&lstats->counts, &all_zeroes, sizeof(PgStat_TableCounts)) == 0)
        return true;

    // Try to acquire lock
    if (!pgstat_lock_entry(entry_ref, nowait))
        return false;

    // Get shared statistics entry and update counters
    tabentry = &((PgStatShared_Relation *) entry_ref->shared_stats)->stats;

    // Update scan statistics
    tabentry->numscans += lstats->counts.numscans;
    if (lstats->counts.numscans)
        tabentry->lastscan = GetCurrentTransactionStopTimestamp();

    // Update tuple operation counters
    tabentry->tuples_returned += lstats->counts.tuples_returned;
    tabentry->tuples_fetched += lstats->counts.tuples_fetched;
    tabentry->tuples_inserted += lstats->counts.tuples_inserted;
    tabentry->tuples_updated += lstats->counts.tuples_updated;
    tabentry->tuples_deleted += lstats->counts.tuples_deleted;
    tabentry->tuples_hot_updated += lstats->counts.tuples_hot_updated;

    // Handle truncated/dropped tables by resetting counters
    if (lstats->counts.truncdropped)
    {
        tabentry->live_tuples = 0;
        tabentry->dead_tuples = 0;
        tabentry->ins_since_vacuum = 0;
    }

    // Update live/dead tuple counts with clamping to prevent negatives
    tabentry->live_tuples = Max(tabentry->live_tuples + lstats->counts.delta_live_tuples, 0);
    tabentry->dead_tuples = Max(tabentry->dead_tuples + lstats->counts.delta_dead_tuples, 0);

    // Update analysis and vacuum tracking
    tabentry->mod_since_analyze += lstats->counts.changed_tuples;
    tabentry->ins_since_vacuum += lstats->counts.tuples_inserted;

    // Update buffer access statistics
    tabentry->blocks_fetched += lstats->counts.blocks_fetched;
    tabentry->blocks_hit += lstats->counts.blocks_hit;

    pgstat_unlock_entry(entry_ref);

    // Also update database-level aggregate statistics
    PgStat_StatDBEntry *dbentry = pgstat_prep_database_pending(dboid);
    dbentry->tuples_returned += lstats->counts.tuples_returned;
    dbentry->tuples_fetched += lstats->counts.tuples_fetched;
    dbentry->tuples_inserted += lstats->counts.tuples_inserted;
    dbentry->tuples_updated += lstats->counts.tuples_updated;
    dbentry->tuples_deleted += lstats->counts.tuples_deleted;
    dbentry->blocks_fetched += lstats->counts.blocks_fetched;
    dbentry->blocks_hit += lstats->counts.blocks_hit;

    return true;
}
```