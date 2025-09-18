# RelationCacheInvalidate

## Location
src/backend/utils/cache/relcache.c: 3013 - 3114

## Overview
Performs a comprehensive invalidation of the relation cache, destroying unused cache entries and rebuilding active ones, typically used to recover from shared invalidation message buffer overflow.

## Definition
```c
void RelationCacheInvalidate(bool debug_discard)
```

## Detailed Description
RelationCacheInvalidate is a powerful cache management function that performs a wholesale invalidation and rebuilding of the relation cache. It is primarily used to recover from SI (Shared Invalidation) message buffer overflow situations where individual invalidation messages cannot be processed reliably.

The function operates in a carefully designed two-phase approach:

**Phase 1 - Deletion and Classification:**
- Reloads relation mapping data first
- Walks through all cached relations using hash_seq_search
- Skips relations with new-in-transaction relfilenumbers (they can't be targets of cross-backend updates)
- Immediately deletes relations with zero reference counts
- For relations with positive reference counts, updates mapped relation physical addresses and classifies them for rebuilding

**Phase 2 - Rebuilding:**
- Rebuilds relations in a specific order: pg_class first, pg_class_oid_index second, other nailed relations next, then everything else
- This ordering ensures system catalogs are available before attempting to reload other relations

The function also handles SMgrRelation cleanup and optionally signals any in-progress RelationBuildDesc() operations to restart.

## Parameters / Member Variables
- `debug_discard`: If true, indicates this is being called from debug_discard_caches and prevents signaling RelationBuildDesc() to restart (avoiding infinite loops)

## Dependencies
- Functions called/Symbols referenced:
  - RelationMapInvalidateAll
  - hash_seq_init
  - hash_seq_search
  - RelationHasReferenceCountZero
  - RelationClearRelation
  - RelationIsMapped
  - RelationCloseSmgr
  - RelationInitPhysicalAddr
  - smgrreleaseall
  - lcons
  - lappend
  - list_free
- Called from (representative examples):
  - InvalidateSystemCachesExtended
  - LocalExecuteInvalidationMessage

## Notes and Other Information
- The two-phase approach is critical for safety with hash_seq_search, which only handles concurrent deletion of the currently visited element
- Relations are rebuilt in a specific priority order to ensure system catalogs are available first
- The function updates relfilenumbers for mapped relations during phase 1 to prevent inconsistencies
- Designed to handle recursive invocation safely due to the two-phase design
- Primarily used for SI message buffer overflow recovery, but also used by debug_discard_caches
- Does not affect relations created in the current transaction, as they cannot be targets of cross-backend invalidation messages