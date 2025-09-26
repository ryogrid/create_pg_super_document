# PgStat_EntryRefHashEntry

## Location
src/backend/utils/activity/pgstat_shmem.c: 24 - 29

## Overview
A hash table entry structure used to map statistical keys to their corresponding entry references in PostgreSQL's statistics collection system.

## Definition
```c
typedef struct PgStat_EntryRefHashEntry
{
    PgStat_HashKey key;         /* hash key */
    char           status;      /* for simplehash use */
    PgStat_EntryRef *entry_ref;
} PgStat_EntryRefHashEntry;
```

## Detailed Description
`PgStat_EntryRefHashEntry` is a fundamental component of PostgreSQL's statistics collection infrastructure, serving as an entry in the backend-local hash table (`pgStatEntryRefHash`) that maps statistical object keys to their corresponding entry references. This structure acts as a performance optimization layer in front of the shared statistics hash table, reducing contention by allowing most statistics operations to occur without directly accessing the shared memory structures.

The hash table containing these entries is created using PostgreSQL's simplehash infrastructure, which requires specific member fields including the hash key and status field. This local caching mechanism significantly improves performance by maintaining references to frequently accessed statistics objects without requiring repeated shared memory lookups.

## Parameters / Member Variables
- `key`: A `PgStat_HashKey` structure that uniquely identifies the statistics object, containing the statistics kind, database OID, and object OID
- `status`: A status field required by the simplehash implementation for managing hash table operations and entry states
- `entry_ref`: A pointer to the actual `PgStat_EntryRef` structure that contains the backend-local reference to the shared statistics entry

## Dependencies
- Types referenced:
  - PgStat_HashKey (hash key structure identifying statistics objects)
  - PgStat_EntryRef (backend-local reference to shared statistics entry)
- Used by (representative examples):
  - SH_ELEMENT_TYPE (simplehash macro definition)
  - pgstat_get_entry_ref_cached (cached entry reference retrieval)
  - pgstat_gc_entry_refs (garbage collection of entry references)
  - pgstat_release_matching_entry_refs (selective entry reference release)
  - pgstat_drop_entry (statistics entry deletion)

## Notes and Other Information
This structure is integral to PostgreSQL's two-tiered statistics architecture, where backend-local hash tables (using this entry type) provide fast access to shared statistics data. The simplehash infrastructure used requires the `status` field for internal hash table management. The structure is defined in `src/backend/utils/activity/pgstat_shmem.c` and is used extensively throughout the statistics collection subsystem for efficient local caching of statistics references.