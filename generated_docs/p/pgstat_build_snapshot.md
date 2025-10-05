# pgstat_build_snapshot

## Location
[src/backend/utils/activity/pgstat.c:978-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L978-L1065)

## Overview
Builds a comprehensive snapshot of all PostgreSQL statistics by copying variable stats from shared memory and building snapshots for all fixed-numbered statistics kinds.

## Definition

```c
static void
pgstat_build_snapshot(void)
```
## Detailed Description
This function creates a complete snapshot of the PostgreSQL statistics system by iterating through all variable statistics stored in shared memory and building snapshots for all fixed-numbered statistics kinds. It operates only when snapshot consistency mode is required and avoids rebuilding if a snapshot already exists.

The function performs a two-phase snapshot building process: first, it iterates through the shared hash table to capture all variable statistics entries (filtering by database access permissions and excluding dropped entries), then it processes all fixed-numbered statistics kinds through dedicated snapshot building. Each variable statistics entry is copied from shared memory to the local snapshot context under appropriate locking to ensure data consistency.

The snapshot includes only statistics relevant to the current database context, respecting access permissions and database boundaries as defined by each statistics kind's configuration.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_snapshot](pgstat_prep_snapshot.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [dshash_seq_init](../d/dshash_seq_init.md)
  - [dshash_seq_next](../d/dshash_seq_next.md)
  - [dshash_seq_term](../d/dshash_seq_term.md)
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - pgstat_snapshot_insert
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [pgstat_get_entry_data](pgstat_get_entry_data.md)
  - [pg_atomic_read_u32](pg_atomic_read_u32.md)
  - [dsa_get_address](../d/dsa_get_address.md)
  - [pgstat_build_snapshot_fixed](pgstat_build_snapshot_fixed.md)
- Called from (representative examples):
  - [pgstat_fetch_entry](pgstat_fetch_entry.md)
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)

## Notes and Other Information
- Static function for internal use within the pgstat module
- Only operates when PGSTAT_FETCH_CONSISTENCY_SNAPSHOT mode is active
- Implements database filtering to include only relevant statistics entries
- Uses LWLock protection when copying data from shared memory to ensure consistency
- Creates a timestamp marker for the snapshot to track when it was built
- Skips dropped statistics entries and validates reference counts
- Processes both variable and fixed-numbered statistics in a comprehensive manner
- Sets the snapshot mode flag upon successful completion to prevent redundant rebuilding

## Simplified Source

```c
static void pgstat_build_snapshot(void) {
    // Validate snapshot mode is required
    Assert(pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT);

    // Skip if snapshot already built
    if (pgStatLocal.snapshot.mode == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT)
        return;

    // Prepare snapshot infrastructure
    pgstat_prep_snapshot();
    pgStatLocal.snapshot.snapshot_timestamp = GetCurrentTimestamp();

    // Snapshot all variable stats from shared hash
    dshash_seq_status hstat;
    dshash_seq_init(&hstat, pgStatLocal.shared_hash, false);

    PgStatShared_HashEntry *p;
    while ((p = dshash_seq_next(&hstat)) != NULL) {
        PgStat_Kind kind = p->key.kind;
        const PgStat_KindInfo *kind_info = pgstat_get_kind_info(kind);

        // Filter by database access permissions
        if (p->key.dboid != MyDatabaseId &&
            p->key.dboid != InvalidOid &&
            !kind_info->accessed_across_databases)
            continue;

        if (p->dropped)
            continue;

        // Copy stats data to snapshot with locking
        PgStatShared_Common *stats_data = dsa_get_address(pgStatLocal.dsa, p->body);
        bool found;
        PgStat_SnapshotEntry *entry = pgstat_snapshot_insert(pgStatLocal.snapshot.stats,
                                                            p->key, &found);

        entry->data = MemoryContextAlloc(pgStatLocal.snapshot.context,
                                        kind_info->shared_size);

        LWLockAcquire(&stats_data->lock, LW_SHARED);
        memcpy(entry->data, pgstat_get_entry_data(kind, stats_data),
               kind_info->shared_size);
        LWLockRelease(&stats_data->lock);
    }
    dshash_seq_term(&hstat);

    // Build snapshots for all fixed-numbered stats
    for (int kind = PGSTAT_KIND_FIRST_VALID; kind <= PGSTAT_KIND_LAST; kind++) {
        const PgStat_KindInfo *kind_info = pgstat_get_kind_info(kind);
        if (kind_info->fixed_amount)
            pgstat_build_snapshot_fixed(kind);
    }

    pgStatLocal.snapshot.mode = PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
}
```