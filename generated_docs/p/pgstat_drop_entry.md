# pgstat_drop_entry

## Location
[src/backend/utils/activity/pgstat_shmem.c:927-970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L927-L970)

## Overview
This function drops a single statistics entry from both the local reference cache and shared statistics hash table, with special handling for database entries.

## Definition
```c
bool pgstat_drop_entry(PgStat_Kind kind, Oid dboid, Oid objoid)
```

## Detailed Description
The `pgstat_drop_entry` function removes a statistics entry identified by its kind, database OID, and object OID. It performs a two-step cleanup process: first removing any local backend reference to the entry, then marking the entry as deleted in the shared hash table and attempting to free it. If the entry represents a database (PGSTAT_KIND_DATABASE), it triggers a cascade deletion of all contained statistics entries. The function returns a boolean indicating whether the entry was successfully freed.

## Parameters / Member Variables
- `kind`: The type of statistics entry (PgStat_Kind enum value)
- `dboid`: The Object Identifier of the database containing the entry
- `objoid`: The Object Identifier of the specific object whose statistics are being dropped

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_entry_ref_hash_lookup
  - [pgstat_release_entry_ref](pgstat_release_entry_ref.md)
  - [dshash_find](../d/dshash_find.md)
  - [pgstat_drop_entry_internal](pgstat_drop_entry_internal.md)
  - [pgstat_drop_database_and_contents](pgstat_drop_database_and_contents.md)
- Types used:
  - [PgStat_Kind](../P/PgStat_Kind.md)
  - [PgStat_HashKey](../P/PgStat_HashKey.md)
  - [PgStatShared_HashEntry](../P/PgStatShared_HashEntry.md)
  - [PgStat_EntryRefHashEntry](../P/PgStat_EntryRefHashEntry.md)
- Constants used:
  - PGSTAT_KIND_DATABASE
- Called from (representative examples):
  - [pgstat_init_function_usage](pgstat_init_function_usage.md)
  - [pgstat_drop_replslot](pgstat_drop_replslot.md)
  - [AtEOXact_PgStat_DroppedStats](../A/AtEOXact_PgStat_DroppedStats.md)
  - [AtEOSubXact_PgStat_DroppedStats](../A/AtEOSubXact_PgStat_DroppedStats.md)
  - [pgstat_execute_transactional_drops](pgstat_execute_transactional_drops.md)

## Notes and Other Information
- Returns false if the stats entry could not be freed, true otherwise
- Callers should call pgstat_request_entry_refs_gc() if the entry could not be freed
- Special cascade behavior for database statistics - dropping a database entry triggers deletion of all contained object statistics
- Handles both local reference cleanup and shared memory cleanup
- Part of PostgreSQL's transactional statistics system
- Location: src/backend/utils/activity/pgstat_shmem.c:927-970

## Simplified Source

```c
// Simplified version of pgstat_drop_entry
bool pgstat_drop_entry(PgStat_Kind kind, Oid dboid, Oid objoid) {
    PgStat_HashKey key;
    PgStatShared_HashEntry *shent;
    bool freed = true;

    // Build hash key for the statistics entry
    memset(&key, 0, sizeof(key));
    key.kind = kind;
    key.dboid = dboid;
    key.objoid = objoid;

    // Clean up local reference if it exists
    if (pgStatEntryRefHash) {
        PgStat_EntryRefHashEntry *local_ref =
            pgstat_entry_ref_hash_lookup(pgStatEntryRefHash, key);
        if (local_ref) {
            pgstat_release_entry_ref(local_ref->key, local_ref->entry_ref, true);
        }
    }

    // Find and drop the shared entry
    shent = dshash_find(pgStatLocal.shared_hash, &key, true);
    if (shent) {
        freed = pgstat_drop_entry_internal(shent, NULL);

        // Special case: dropping database also drops all contained stats
        if (key.kind == PGSTAT_KIND_DATABASE) {
            pgstat_drop_database_and_contents(key.dboid);
        }
    }

    return freed;
}
```

Key simplifications made:
- Removed detailed comments and function header documentation for brevity
- Simplified variable names (lohashent → local_ref)
- Added inline comments to explain each logical step
- Preserved all essential logic including local cleanup, shared entry removal, and database cascade deletion
- Maintained the two-phase cleanup approach (local references first, then shared memory)