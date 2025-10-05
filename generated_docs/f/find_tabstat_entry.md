# find_tabstat_entry

## Location
[src/backend/utils/activity/pgstat_relation.c:487-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L487-L536)

## Overview
Finds and returns a copy of an existing PgStat_TableStatus entry for a given relation ID, with subtransaction counters properly accumulated.

## Definition

```c
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
```
## Detailed Description
This function searches for an existing PgStat_TableStatus entry for the specified relation ID in the current database's statistics tracking system. If found in the current database, it uses that entry; otherwise, it searches in shared tables. The function creates a copy of the found entry and accumulates any pending subtransaction statistics into the main counters (tuples_inserted, tuples_updated, tuples_deleted).

The function ensures that live subtransaction counts are properly reconciled into the returned copy, making it safe for the caller to use without worrying about incomplete statistics. The returned copy is allocated using palloc() and should be freed by the caller when no longer needed.

## Parameters / Member Variables
- `rel_id`: Object ID of the relation for which to find statistics entry
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_pending_entry](../p/pgstat_fetch_pending_entry.md) (to locate statistics entries)
  - [palloc](../p/palloc.md) (for memory allocation)
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (statistics entry reference structure)
  - [PgStat_TableXactStatus](../P/PgStat_TableXactStatus.md) (transaction-level table statistics)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md) (table statistics structure)
  - PGSTAT_KIND_RELATION (statistics kind constant)
- Called from (representative examples):
  - PG_STAT_GET_XACT_RELENTRY_INT64 (in pgstatfuncs.c)
  - pgstat_count_buffer_hit (in pgstat.h)

## Notes and Other Information
- Returns NULL if no statistics entry is found for the relation
- The returned PgStat_TableStatus copy has its trans field reset to NULL to avoid pointing to shared memory
- This function reconciles subtransaction statistics even if the caller may not need all the data
- Memory management: The caller is responsible for freeing the returned PgStat_TableStatus using pfree()
- The function first searches in the current database, then falls back to shared tables for system relations

## Simplified Source

```c
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
    PgStat_EntryRef *entry_ref;
    PgStat_TableStatus *tabentry = NULL;
    PgStat_TableStatus *tablestatus = NULL;

    // Try to find entry in current database first
    entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, MyDatabaseId, rel_id);
    if (!entry_ref)
    {
        // If not found, try shared tables
        entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, InvalidOid, rel_id);
        if (!entry_ref)
            return NULL;
    }

    // Create a copy of the found entry
    tabentry = (PgStat_TableStatus *) entry_ref->pending;
    tablestatus = palloc(sizeof(PgStat_TableStatus));
    *tablestatus = *tabentry;

    // Clear shared memory pointer in copy
    tablestatus->trans = NULL;

    // Accumulate subtransaction counters into main counters
    for (PgStat_TableXactStatus *trans = tabentry->trans; trans != NULL; trans = trans->upper)
    {
        tablestatus->counts.tuples_inserted += trans->tuples_inserted;
        tablestatus->counts.tuples_updated += trans->tuples_updated;
        tablestatus->counts.tuples_deleted += trans->tuples_deleted;
    }

    return tablestatus;
}
```