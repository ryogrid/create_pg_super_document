# InvalidateEventCacheCallback

## Location
src/backend/utils/cache/evtcache.c: 255 - 270

## Overview
A system cache invalidation callback function that marks the event trigger cache for rebuild when the pg_event_trigger system catalog is modified.

## Definition
```c
static void InvalidateEventCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
```

## Detailed Description
InvalidateEventCacheCallback is a static callback function registered with PostgreSQL's system cache invalidation mechanism to handle changes to the pg_event_trigger system catalog. When the catalog is modified (through CREATE EVENT TRIGGER, DROP EVENT TRIGGER, or ALTER EVENT TRIGGER commands), this callback is automatically invoked to invalidate the event trigger cache. The function implements a safe invalidation strategy that respects ongoing cache rebuild operations while ensuring the cache is marked for reconstruction.

The callback uses a simple but effective approach: if the cache is currently valid, it immediately frees the cache memory and sets the cache pointer to NULL. Regardless of the current cache state, it always marks the cache state as needing rebuild. This design prevents memory leaks while handling concurrent operations safely.

## Parameters / Member Variables
- `arg`: A Datum argument passed when the callback was registered (unused in this implementation)
- `cacheid`: The system cache identifier that triggered the invalidation (should be EVENTTRIGGEROID)
- `hashvalue`: The hash value of the invalidated cache entry (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextReset
  - ETCS_VALID, ETCS_NEEDS_REBUILD (cache state constants)
- Called from (representative examples):
  - Registered as callback in BuildEventTriggerCache (src/backend/utils/cache/evtcache.c:109)

## Notes and Other Information
- Registered with CacheRegisterSyscacheCallback during first cache build using EVENTTRIGGEROID
- Uses a coarse-grained invalidation strategy - invalidates entire cache rather than individual entries
- Safe to call during cache rebuild operations due to state checking (ETCS_VALID)
- Prevents memory leaks by immediately freeing cache memory when possible
- Part of PostgreSQL's general cache invalidation infrastructure
- Automatically triggered by DDL operations on event triggers
- Ensures cache consistency after catalog modifications
- Simple design trades granularity for reliability and simplicity