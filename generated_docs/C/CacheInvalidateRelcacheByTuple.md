# CacheInvalidateRelcacheByTuple

## Location
[src/backend/utils/cache/inval.c:1399-1421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1399-L1421)

## Overview
Registers invalidation of a relation's relcache entry by identifying the relation through its pg_class tuple rather than a Relation structure.

## Definition

```c
void
CacheInvalidateRelcacheByTuple(HeapTuple classTuple)
```
## Detailed Description
CacheInvalidateRelcacheByTuple provides the same functionality as CacheInvalidateRelcache but operates on a pg_class tuple instead of a Relation structure. This is useful in contexts where you have access to the catalog tuple but not necessarily an open relation. The function extracts the relation OID and shared status directly from the pg_class tuple structure and registers the appropriate relcache invalidation.

This variant is particularly useful during catalog operations where relations are being manipulated at the tuple level, such as during system table updates or when processing catalog changes where the relation may not be currently open.

## Parameters / Member Variables
- : HeapTuple representing a row from the pg_class system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_class (type cast)
  - GETSTRUCT (macro)
  - [PrepareInvalidationState](../P/PrepareInvalidationState.md)
  - [RegisterRelcacheInvalidation](../R/RegisterRelcacheInvalidation.md)
- Called from (representative examples):
  - [CacheInvalidateRelcacheByRelid](CacheInvalidateRelcacheByRelid.md)
  - [index_update_stats](../i/index_update_stats.md)
  - [copy_table_data](../c/copy_table_data.md)
  - [swap_relation_files](../s/swap_relation_files.md)
  - [SetRelationHasSubclass](../S/SetRelationHasSubclass.md)

## Notes and Other Information
- This function is essentially a wrapper that extracts relation information from a pg_class tuple and calls the standard relcache invalidation mechanism
- It handles both shared and database-specific relations by examining the relisshared field in the tuple
- Commonly used in low-level catalog manipulation functions where tuple-level access is more natural than relation-level access
- The classTuple parameter must be a valid pg_class tuple - no validation is performed on the input
- Often used as an intermediate function by other invalidation routines that work with relation OIDs