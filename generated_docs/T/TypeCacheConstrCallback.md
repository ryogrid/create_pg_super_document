# TypeCacheConstrCallback

## Location
[src/backend/utils/cache/typcache.c:2424-2447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2424-L2447)

## Overview
TypeCacheConstrCallback is a syscache invalidation callback function that handles updates to the pg_constraint system catalog by invalidating cached domain constraint information in the type cache.

## Definition
static void TypeCacheConstrCallback(Datum arg, int cacheid, uint32 hashvalue)

## Detailed Description
This function serves as a callback that is invoked whenever a syscache invalidation event occurs for any row in the pg_constraint system catalog. The function specifically handles invalidation of cached domain constraint data when constraint definitions change.

Unlike other type cache callbacks, this function uses an optimized approach due to its frequent invocation. Rather than scanning all type cache entries using hash_seq_search, it leverages a threaded list of domain-type entries (linked via the nextDomain field) to efficiently visit only the relevant entries. This optimization is important because domain types are typically a small subset of all cached types, and constraint updates occur frequently.

The function cannot distinguish between domain constraint updates and table constraint updates, leading to some unnecessary cache flushes when table constraints change. However, this approach is still superior to the previous design that did not cache domain constraints at all.

## Parameters / Member Variables
- `arg`: Datum argument passed by the syscache callback mechanism (unused in this function)
- `cacheid`: Cache identifier indicating which syscache triggered the invalidation (unused in this function)
- `hashvalue`: Hash value associated with the invalidated entry (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS
  - firstDomainTypeEntry (global variable)
  - nextDomain (TypeCacheEntry field)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (callback registration)

## Notes and Other Information
- This is a static function, only accessible within typcache.c
- Uses an optimized traversal method via threaded domain entry list rather than hash table scan
- Called frequently, so performance optimization is important
- Cannot distinguish between domain and table constraint invalidations, leading to some unnecessary work
- Part of PostgreSQL's type cache invalidation mechanism for maintaining constraint data consistency

## Simplified Source

```c
static void
TypeCacheConstrCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    TypeCacheEntry *typentry;

    // Efficiently traverse only domain-type entries using threaded list
    for (typentry = firstDomainTypeEntry;
         typentry != NULL;
         typentry = typentry->nextDomain)
    {
        // Reset domain constraint validity information
        typentry->flags &= ~TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS;
    }
}
```