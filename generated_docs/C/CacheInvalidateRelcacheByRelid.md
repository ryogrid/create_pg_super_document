# CacheInvalidateRelcacheByRelid

## Location
[src/backend/utils/cache/inval.c:1422-1461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1422-L1461)

## Overview
Registers invalidation of a relation's relcache entry by identifying the relation through its OID, providing the least efficient but most convenient interface when only the relation OID is available.

## Definition

```c
void
CacheInvalidateRelcacheByRelid(Oid relid)
```
## Detailed Description
CacheInvalidateRelcacheByRelid provides relcache invalidation functionality when only a relation OID is available. This is the least efficient of the three relcache invalidation variants because it must perform a system catalog lookup to retrieve the pg_class tuple before proceeding with the invalidation. The function searches the system cache for the relation tuple, validates it exists, then delegates to CacheInvalidateRelcacheByTuple to perform the actual invalidation.

Despite being less efficient, this function is often the most convenient to use in contexts where only the relation OID is known and obtaining a Relation structure or pg_class tuple would require additional overhead.

## Parameters / Member Variables
- : The OID of the relation whose relcache entry should be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - [PrepareInvalidationState](../P/PrepareInvalidationState.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - [CacheInvalidateRelcacheByTuple](CacheInvalidateRelcacheByTuple.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md)
  - [ATExecAlterConstraint](../A/ATExecAlterConstraint.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)

## Notes and Other Information
- This is explicitly noted as the least efficient of the three relcache invalidation options due to the required catalog lookup
- The function performs error checking and will throw an ERROR if the relation OID doesn't exist in pg_class
- Uses the system cache (syscache) for efficient catalog tuple retrieval and properly releases the cache entry after use
- Commonly used in DDL operations where relation OIDs are the primary available identifier
- The syscache lookup uses RELOID cache for efficient access to pg_class tuples by OID
- Should be avoided in performance-critical paths if a Relation structure or pg_class tuple is already available