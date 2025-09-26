# RelationCacheInitializePhase2

## Location
[src/backend/utils/cache/relcache.c:4043-4080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4043-L4080)

## Overview
RelationCacheInitializePhase2 prepares access to shared catalogs during PostgreSQL startup by setting up relation descriptors for critical shared system catalogs.

## Definition

```c
void
RelationCacheInitializePhase2(void)
```
## Detailed Description
RelationCacheInitializePhase2 is the second phase of relation cache initialization that specifically handles shared catalogs. This function is called during backend startup to ensure that PostgreSQL can access the shared system catalogs necessary for user authentication and database access.

The function first attempts to load relation descriptors from the shared relcache init file, which is a performance optimization that avoids re-reading catalog information from disk. If the shared init file is missing or corrupted, the function falls back to creating minimal "phony" relation descriptors for the essential shared catalogs using formrdesc().

The critical shared catalogs that must be available include:
- pg_database (information about databases)  
- pg_authid (user/role authentication information)
- pg_auth_members (role membership information)
- pg_shseclabel (shared security labels)
- pg_subscription (logical replication subscriptions)

This initialization is essential because these catalogs are needed before a backend can fully connect to a specific database. RelationCacheInitializePhase3 will later complete the initialization process once the transaction system is fully operational.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - RelationMapInitializePhase2
  - IsBootstrapProcessingMode
  - MemoryContextSwitchTo
  - load_relcache_init_file
  - formrdesc
  - CacheMemoryContext
- Called from (representative examples):
  - InitPostgres (main caller during backend initialization)

## Notes and Other Information
- Skips initialization entirely during bootstrap mode since shared catalogs don't exist yet
- Operates in CacheMemoryContext to ensure relation descriptors persist for the backend lifetime
- Part of the multi-phase relation cache initialization sequence (Phase1 → Phase2 → Phase3)
- The NUM_CRITICAL_SHARED_RELS constant (5) must be updated if the list of critical shared catalogs changes
- Failure to load the shared init file is not fatal - the function gracefully falls back to manual descriptor creation
- This phase specifically handles shared catalogs while Phase3 handles regular database-specific catalogs