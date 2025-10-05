# pgstat_prep_snapshot

## Location
[src/backend/utils/activity/pgstat.c:957-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L957-L977)

## Overview
Prepares the statistics snapshot infrastructure by creating the memory context and hash table needed for snapshot operations if they don't already exist.

## Definition

```c
static void
pgstat_prep_snapshot(void)
```
## Detailed Description
This internal function initializes the snapshot infrastructure required for statistics data collection and storage. It ensures that the necessary memory context and statistics hash table are properly set up before snapshot building operations commence. The function operates conditionally, only performing initialization when specific conditions are met, making it safe to call multiple times.

The function handles forced snapshot clearing, checks fetch consistency settings, and creates the snapshot memory context using a small allocation set strategy optimized for statistics data structures. It establishes the hash table that will store the actual statistics entries during snapshot operations.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_clear_snapshot](pgstat_clear_snapshot.md)
  - AllocSetContextCreate
  - pgstat_snapshot_create (macro/function)
  - PGSTAT_FETCH_CONSISTENCY_NONE
  - ALLOCSET_SMALL_SIZES
  - PGSTAT_SNAPSHOT_HASH_SIZE
- Called from (representative examples):
  - [pgstat_fetch_entry](pgstat_fetch_entry.md)
  - [pgstat_build_snapshot](pgstat_build_snapshot.md)

## Notes and Other Information
- Static function for internal use within the pgstat module
- Implements lazy initialization - only creates structures when needed
- Respects fetch consistency settings to avoid unnecessary work
- Uses TopMemoryContext as parent for the snapshot context to ensure proper lifetime management
- The snapshot context uses ALLOCSET_SMALL_SIZES for memory-efficient allocation of statistics entries
- Safe to call multiple times due to conditional initialization logic

## Simplified Source

```c
static void pgstat_prep_snapshot(void) {
    // Clear snapshot if forced
    if (force_stats_snapshot_clear)
        pgstat_clear_snapshot();

    // Skip if no consistency needed or snapshot already exists
    if (pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE ||
        pgStatLocal.snapshot.stats != NULL)
        return;

    // Create memory context if needed
    if (!pgStatLocal.snapshot.context)
        pgStatLocal.snapshot.context = AllocSetContextCreate(TopMemoryContext,
                                                            "PgStat Snapshot",
                                                            ALLOCSET_SMALL_SIZES);

    // Create statistics hash table
    pgStatLocal.snapshot.stats = pgstat_snapshot_create(pgStatLocal.snapshot.context,
                                                        PGSTAT_SNAPSHOT_HASH_SIZE,
                                                        NULL);
}
```