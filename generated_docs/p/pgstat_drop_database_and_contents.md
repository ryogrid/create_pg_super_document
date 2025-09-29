# pgstat_drop_database_and_contents

## Location
[src/backend/utils/activity/pgstat_shmem.c:866-926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L866-L926)

## Overview
This function drops statistics for a database and all objects contained within that database from the shared statistics hash table.

## Definition

```c
struct PgStat_HashKey));
```
## Detailed Description
The  function performs a comprehensive cleanup of statistics data for a specific database. It iterates through the shared statistics hash table and removes all entries that belong to the specified database OID. The function implements a two-phase approach: first releasing local backend references to prevent cleanup delays, then performing the actual removal while holding appropriate locks.

The function handles cases where statistics entries cannot be immediately freed (for example, when they are still being accessed by other backends) by incrementing a counter and requesting garbage collection of cached references when needed.

## Parameters / Member Variables
- : The Object Identifier (OID) of the database whose statistics entries should be dropped

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_release_db_entry_refs](pgstat_release_db_entry_refs.md)
  - [dshash_seq_init](../d/dshash_seq_init.md)
  - [dshash_seq_next](../d/dshash_seq_next.md)
  - [pgstat_drop_entry_internal](pgstat_drop_entry_internal.md)
  - [dshash_seq_term](../d/dshash_seq_term.md)
  - [pgstat_request_entry_refs_gc](pgstat_request_entry_refs_gc.md)
- Types used:
  - [dshash_seq_status](../d/dshash_seq_status.md)
  - [PgStatShared_HashEntry](../P/PgStatShared_HashEntry.md)
- Called from:
  - [pgstat_drop_entry](pgstat_drop_entry.md)

## Notes and Other Information
- This is a static function internal to pgstat_shmem.c
- Uses exclusive locking on the shared hash table during iteration to ensure thread safety
- Implements garbage collection signaling for entries that cannot be immediately freed
- Part of PostgreSQL's statistics collection infrastructure
- Location: src/backend/utils/activity/pgstat_shmem.c:866-926

## Simplified Source

```c
static void
pgstat_drop_database_and_contents(Oid dboid)
{
    dshash_seq_status hstat;
    PgStatShared_HashEntry *p;
    uint64 not_freed_count = 0;

    Assert(OidIsValid(dboid));
    Assert(pgStatLocal.shared_hash != NULL);

    // Release local references first to avoid cleanup delays
    pgstat_release_db_entry_refs(dboid);

    // Iterate through shared hash table with exclusive lock
    dshash_seq_init(&hstat, pgStatLocal.shared_hash, true);
    while ((p = dshash_seq_next(&hstat)) != NULL)
    {
        // Skip already dropped entries
        if (p->dropped)
            continue;

        // Skip entries not belonging to this database
        if (p->key.dboid != dboid)
            continue;

        // Try to drop the entry
        if (!pgstat_drop_entry_internal(p, &hstat))
        {
            // Count entries that couldn't be freed immediately
            not_freed_count++;
        }
    }
    dshash_seq_term(&hstat);

    // If some entries couldn't be freed, request garbage collection
    if (not_freed_count > 0)
        pgstat_request_entry_refs_gc();
}
```