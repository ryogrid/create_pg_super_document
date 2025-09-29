# RelationCacheInvalidateEntry

## Location
[src/backend/utils/cache/relcache.c:2957-3012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2957-L3012)

## Overview
Invalidates a specific relation cache entry in response to shared invalidation (SI) cache flush messages, ensuring cache consistency across database backends.

## Definition
```c
void RelationCacheInvalidateEntry(Oid relationId)
```

## Detailed Description
RelationCacheInvalidateEntry is invoked as part of PostgreSQL's shared invalidation system to handle cache flush messages. When a relation is modified by one backend, all other backends need to invalidate their cached copies of that relation's metadata to maintain consistency.

The function operates in two scenarios:
1. If the relation is currently in the cache, it flushes the relation using RelationFlushRelation and increments the relcacheInvalsReceived counter for statistics
2. If the relation is not in the cache but is currently being built (exists in the in_progress_list), it marks that entry as invalidated to prevent stale data from being cached

This dual approach ensures that both cached relations and relations currently being loaded are properly handled during invalidation events.

## Parameters / Member Variables
- `relationId`: The OID of the relation to invalidate from the cache

## Dependencies
- Functions called/Symbols referenced:
  - RelationIdCacheLookup
  - PointerIsValid
  - [RelationFlushRelation](RelationFlushRelation.md)
- Called from (representative examples):
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)

## Notes and Other Information
- Part of PostgreSQL's shared invalidation (SI) system for maintaining cache consistency across multiple backends
- The function processes both local and non-local relations, unlike earlier versions that skipped local relations
- Maintains statistics through the relcacheInvalsReceived counter
- Handles the special case of relations currently being built through the in_progress_list mechanism
- The caller is responsible for determining that the relation belongs to the current database or is a shared relation before calling this function

## Simplified Source

```c
// Invalidate a specific relation cache entry for SI flush messages
void RelationCacheInvalidateEntry(Oid relationId)
{
    Relation relation;

    // Look up the relation in the cache
    RelationIdCacheLookup(relationId, relation);

    if (PointerIsValid(relation))
    {
        // If cached, flush it and update statistics
        relcacheInvalsReceived++;
        RelationFlushRelation(relation);
    }
    else
    {
        // If not cached but being built, mark as invalidated
        int i;
        for (i = 0; i < in_progress_list_len; i++)
            if (in_progress_list[i].reloid == relationId)
                in_progress_list[i].invalidated = true;
    }
}
```