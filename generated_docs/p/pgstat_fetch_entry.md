# pgstat_fetch_entry

## Location
[src/backend/utils/activity/pgstat.c:811-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L811-L906)

## Overview
This function fetches statistics data for a specific database object identified by kind, database OID, and object OID, handling different consistency levels and caching strategies.

## Definition

```c
struct PgStat_HashKey));
```
## Detailed Description
The  function is a core component of PostgreSQL's statistics fetching infrastructure that retrieves statistics data for individual database objects. It supports different consistency models ranging from no caching to full snapshot consistency, and handles memory management appropriately for each mode.

The function operates by first constructing a hash key from the provided parameters, then determining the appropriate fetching strategy based on the current  setting. For snapshot consistency, it may build a complete snapshot first. For cache consistency, it maintains cached entries to avoid repeated expensive lookups.

The function handles several edge cases: it returns NULL for dropped entries, creates empty cache entries when appropriate, and manages memory allocation differently depending on the consistency mode to optimize performance and memory usage.

## Parameters / Member Variables
- : A  enum value specifying the type of statistics to fetch
- : The OID of the database containing the object
- : The OID of the specific object whose statistics are being requested

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - [pgstat_prep_snapshot](pgstat_prep_snapshot.md)
  - [pgstat_build_snapshot](pgstat_build_snapshot.md)
  - pgstat_snapshot_lookup
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md)
  - pgstat_snapshot_insert
  - [pgstat_lock_entry_shared](pgstat_lock_entry_shared.md)
  - [pgstat_get_entry_data](pgstat_get_entry_data.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [PgStat_HashKey](../P/PgStat_HashKey.md) (struct type)
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (struct type)
  - [PgStat_SnapshotEntry](../P/PgStat_SnapshotEntry.md) (struct type)
- Called from (representative examples):
  - [pgstat_fetch_stat_dbentry](pgstat_fetch_stat_dbentry.md)
  - [pgstat_fetch_stat_funcentry](pgstat_fetch_stat_funcentry.md)
  - [pgstat_fetch_stat_tabentry_ext](pgstat_fetch_stat_tabentry_ext.md)
  - [pgstat_fetch_replslot](pgstat_fetch_replslot.md)
  - [pgstat_fetch_stat_subscription](pgstat_fetch_stat_subscription.md)

## Notes and Other Information
- This function should only be called from backend processes, not the postmaster
- Only supports statistics kinds with variable amounts (not fixed_amount kinds)
- Memory allocation strategy varies by consistency mode: caller's context for NONE, snapshot context for others
- Handles three consistency levels: NONE (no caching), CACHE (individual entry caching), and SNAPSHOT (full snapshot)
- Thread-safe through proper locking of shared statistics entries
- Returns NULL when no statistics exist for the requested object or if the object has been dropped
- The function clears padding in the hash key structure to ensure consistent hash values

## Simplified Source

```c
void *
pgstat_fetch_entry(PgStat_Kind kind, Oid dboid, Oid objoid)
{
    PgStat_HashKey key;
    PgStat_EntryRef *entry_ref;
    void *stats_data;
    const PgStat_KindInfo *kind_info = pgstat_get_kind_info(kind);

    Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
    Assert(!kind_info->fixed_amount);

    pgstat_prep_snapshot();

    // Build hash key for lookup
    memset(&key, 0, sizeof(struct PgStat_HashKey));
    key.kind = kind;
    key.dboid = dboid;
    key.objoid = objoid;

    // Build full snapshot if required by consistency mode
    if (pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT)
        pgstat_build_snapshot();

    // Check cache if caching is enabled
    if (pgstat_fetch_consistency > PGSTAT_FETCH_CONSISTENCY_NONE) {
        PgStat_SnapshotEntry *entry = pgstat_snapshot_lookup(pgStatLocal.snapshot.stats, key);
        if (entry)
            return entry->data;

        // No data in full snapshot means no stats exist
        if (pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT)
            return NULL;
    }

    pgStatLocal.snapshot.mode = pgstat_fetch_consistency;

    // Get reference to shared statistics entry
    entry_ref = pgstat_get_entry_ref(kind, dboid, objoid, false, NULL);

    if (entry_ref == NULL || entry_ref->shared_entry->dropped) {
        // Create empty cache entry if using cache consistency
        if (pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_CACHE) {
            PgStat_SnapshotEntry *entry = pgstat_snapshot_insert(pgStatLocal.snapshot.stats, key, &found);
            Assert(!found);
            entry->data = NULL;
        }
        return NULL;
    }

    // Allocate memory for statistics data
    if (pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE)
        stats_data = palloc(kind_info->shared_data_len);  // Caller's context
    else
        stats_data = MemoryContextAlloc(pgStatLocal.snapshot.context,
                                       kind_info->shared_data_len);  // Snapshot context

    // Copy data from shared memory with locking
    pgstat_lock_entry_shared(entry_ref, false);
    memcpy(stats_data,
           pgstat_get_entry_data(kind, entry_ref->shared_stats),
           kind_info->shared_data_len);
    pgstat_unlock_entry(entry_ref);

    // Cache the result if caching is enabled
    if (pgstat_fetch_consistency > PGSTAT_FETCH_CONSISTENCY_NONE) {
        PgStat_SnapshotEntry *entry = pgstat_snapshot_insert(pgStatLocal.snapshot.stats, key, &found);
        entry->data = stats_data;
    }

    return stats_data;
}
```