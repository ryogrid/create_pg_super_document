# RelationIdGetRelation

## Location
src/backend/utils/cache/relcache.c: 2063 - 2141

## Overview
Looks up a relation descriptor by OID, either retrieving it from the relation cache or building a new one if not already cached.

## Definition


## Detailed Description
This function is the primary interface for obtaining relation descriptors in PostgreSQL. It implements a two-level lookup strategy:

1. **Cache Lookup**: First attempts to find the relation descriptor in the relation cache using RelationIdCacheLookup().
2. **Cache Miss Handling**: If not found in cache, calls RelationBuildDesc() to build a new relation descriptor from system catalogs.

For cached entries, the function performs several important checks:
- **Dropped Relations**: Returns NULL for relations marked as dropped (rd_droppedSubid != InvalidSubTransactionId)
- **Cache Validation**: For invalid cached entries, applies different revalidation strategies:
  - Indexes: Uses RelationReloadIndexInfo() for limited schema changes
  - Other relations: Uses RelationClearRelation() for full revalidation

The function always increments the relation's reference count, requiring callers to eventually decrement it (usually via RelationClose()).

## Parameters / Member Variables
- : OID of the relation to look up

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionState
  - RelationIdCacheLookup
  - RelationIsValid
  - RelationIncrementReferenceCount
  - RelationReloadIndexInfo
  - RelationClearRelation
  - RelationBuildDesc
- Called from (representative examples):
  - relation_open
  - try_relation_open
  - pgoutput_change
  - RelationGetIdentityKeyBitmap

## Notes and Other Information
- Requires caller to hold at least AccessShareLock on the relation to avoid race conditions
- Returns NULL only if pg_class row is not found (suggesting relation was just deleted)
- All other errors are reported via elog()
- Reference count management is critical - callers must call RelationClose() or equivalent
- Handles special cases during bootstrap when critical relation caches aren't yet built
- Part of PostgreSQL's relation cache management system that provides efficient access to relation metadata
- The function asserts that it's called within a transaction context using IsTransactionState()