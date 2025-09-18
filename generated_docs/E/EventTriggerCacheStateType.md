# EventTriggerCacheStateType

## Location
[src/backend/utils/cache/evtcache.c:38-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/evtcache.c#L38-L43)

## Overview
An enumeration type that represents the current state of the event trigger cache, tracking whether it needs rebuilding, is in the process of being rebuilt, or is currently valid and ready for use.

## Definition


## Detailed Description
 is an enum that manages the lifecycle states of PostgreSQL's event trigger cache system. The event trigger cache stores information about event triggers to avoid repeated database lookups, and this type ensures proper synchronization during cache rebuilds and prevents race conditions or inconsistent states.

The cache operates as a state machine with three distinct phases:
1. **ETCS_NEEDS_REBUILD**: Initial state or after invalidation, indicating the cache requires reconstruction
2. **ETCS_REBUILD_STARTED**: Intermediate state during active cache reconstruction to prevent concurrent rebuilds
3. **ETCS_VALID**: Final state when the cache is complete and ready for queries

This state management is crucial for maintaining data consistency, especially during concurrent access patterns and cache invalidation scenarios.

## Parameters / Member Variables
- : Indicates the cache is invalid and needs to be rebuilt from scratch
- : Marks that a cache rebuild operation is currently in progress, preventing concurrent rebuilds
- : Signifies the cache is fully constructed and ready for lookup operations

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerEvent (used in cache entry structure)
- Called from (representative examples):
  - [EventCacheLookup](EventCacheLookup.md) (checks if cache is valid)
  - [BuildEventTriggerCache](../B/BuildEventTriggerCache.md) (manages state transitions)
  - [InvalidateEventCacheCallback](../I/InvalidateEventCacheCallback.md) (sets to NEEDS_REBUILD)

## Notes and Other Information
- The state is maintained by a static variable  initialized to 
- State transitions follow a strict pattern: NEEDS_REBUILD → REBUILD_STARTED → VALID → NEEDS_REBUILD (on invalidation)
- The intermediate REBUILD_STARTED state prevents memory context destruction during reconstruction
- Cache invalidation can occur while a rebuild is in progress, requiring careful state management to avoid infinite loops
- This type is defined in  as part of the internal cache implementation
- The cache stores event trigger information organized by  types for efficient lookup