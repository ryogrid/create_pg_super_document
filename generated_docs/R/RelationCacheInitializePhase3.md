# RelationCacheInitializePhase3

## Location
[src/backend/utils/cache/relcache.c:4102-4137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4102-L4137)

## Overview
RelationCacheInitializePhase3 completes the relation cache initialization by loading critical system indexes and updating relation descriptors once the catcache and transaction systems are functional.

## Definition

```c
void
RelationCacheInitializePhase3(void)
```
## Detailed Description
RelationCacheInitializePhase3 is the final and most comprehensive phase of relation cache initialization. This function is called once the catcache and transaction systems are fully functional and MyDatabaseId has been determined. At this point, PostgreSQL can actually read data from the database's system catalogs.

The function performs several critical tasks:

1. **Local Cache File Loading**: Attempts to load pre-computed relation cache entries from the local relcache init file. If unsuccessful or in bootstrap mode, it creates minimal relation descriptors for the critical "nailed-in" system catalogs (pg_class, pg_attribute, pg_proc, pg_type).

2. **Critical Index Loading**: Loads critical system indexes that are essential for catalog access. This solves an infinite recursion problem where catcache and opclass cache depend on these indexes for fetches during relcache loading. The function loads both local critical indexes (for catalog access) and shared critical indexes (for authentication and database lookup).

3. **Relation Descriptor Updates**: Scans all existing relcache entries and updates any that might be incorrect from the initial formrdesc calls or cache file. This includes reading real pg_class data to replace fake entries and loading rules, triggers, and security policies.

The function carefully handles the bootstrap case and manages the criticalRelcachesBuilt and criticalSharedRelcachesBuilt flags to control when to use heapscans vs. indexscans during catalog access.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [RelationMapInitializePhase3](RelationMapInitializePhase3.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - IsBootstrapProcessingMode
  - [load_relcache_init_file](../l/load_relcache_init_file.md)
  - [formrdesc](../f/formrdesc.md)
  - [load_critical_index](../l/load_critical_index.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [RelIdCacheEnt](RelIdCacheEnt.md)
  - CacheMemoryContext
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md) (main caller during backend initialization)

## Notes and Other Information
- This is the final phase of the three-phase relation cache initialization sequence
- The function handles an infinite recursion problem by carefully controlling when to use heap scans vs. index scans
- Critical indexes are "nailed in cache" meaning they cannot be flushed and rebuilt once criticalRelcachesBuilt is set to true
- The function restarts hash table scans from scratch after catalog access to handle potential shared-invalidation cache flushes safely
- Local critical indexes (NUM_CRITICAL_LOCAL_INDEXES = 7) include indexes on pg_class, pg_attribute, pg_index, etc.
- Shared critical indexes (NUM_CRITICAL_SHARED_INDEXES = 6) include indexes on pg_database, pg_authid, etc.
- After this phase completes, PostgreSQL has enough infrastructure to open any system catalog or use any catcache
- The last step involves rewriting cache files if needed for future startup performance