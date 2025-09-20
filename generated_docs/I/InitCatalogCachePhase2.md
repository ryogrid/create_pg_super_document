# InitCatalogCachePhase2

## Location
[src/backend/utils/cache/syscache.c:180-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L180-L207)

## Overview
Completes the initialization of PostgreSQL's system catalog caches by performing database access to preload the most commonly-used system catalogs.

## Definition

```c
void
InitCatalogCachePhase2(void)
```
## Detailed Description
InitCatalogCachePhase2 is the second phase of catalog cache initialization that performs actual database access to complete cache setup. Unlike InitCatalogCache which only allocates memory and sets up structures, this function triggers database queries to populate the caches with data.

The function serves a specific optimization purpose: while syscaches can be initialized lazily on first use, this function provides a mechanism to preload the relcache with entries for the most commonly-used system catalogs. This preloading is particularly useful when PostgreSQL needs to write a new relcache init file, as it ensures that critical system catalog information is readily available.

The function iterates through all system caches and calls InitCatCachePhase2() for each one, enabling immediate database access for cache population.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [InitCatCachePhase2](InitCatCachePhase2.md)
- Called from (representative examples):
  - [RelationCacheInitFileRemove](../R/RelationCacheInitFileRemove.md) (in relcache.c)

## Notes and Other Information
- This function is NOT essential for normal PostgreSQL operation - syscaches can initialize on first use
- Primary use case is optimizing relcache init file creation by preloading commonly-used catalogs
- Must be called after InitCatalogCache() has completed (enforced by Assert(CacheInitialized))
- The function performs actual database access, unlike the first initialization phase
- Helps reduce cold-start latency by front-loading cache population during strategic moments
- The 'true' parameter passed to InitCatCachePhase2 likely indicates that database access is permitted