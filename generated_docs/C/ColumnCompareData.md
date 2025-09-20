# ColumnCompareData

## Location
[src/backend/utils/adt/rowtypes.c:54-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L54-L57)

## Overview
A simple structure that caches metadata needed for record column comparison operations by wrapping a TypeCacheEntry pointer.

## Definition

```c
typedef struct ColumnCompareData
{
	TypeCacheEntry *typentry;	/* has everything we need, actually */
} ColumnCompareData;
```
## Detailed Description
ColumnCompareData is a lightweight wrapper structure designed to cache type comparison metadata for individual columns within records. The structure contains a single pointer to a TypeCacheEntry, which holds comprehensive type information including comparison functions, hash functions, and other type-specific metadata needed for comparison operations.

This design leverages PostgreSQL's type cache system, which maintains cached type information to avoid repeated lookups of type metadata from the system catalogs. The comment 'has everything we need, actually' reflects the comprehensive nature of TypeCacheEntry, which contains all the necessary function pointers and type information required for column comparison operations.

The structure serves as a specialized interface for record comparison functions, providing a consistent way to access cached type information specifically in the context of column-by-column record comparisons.

## Parameters / Member Variables
- `*typentry`: Pointer to a TypeCacheEntry structure containing comprehensive type metadata including comparison functions, hash functions, type OID, and other type-specific information needed for comparison operations
## Dependencies
- Functions called/Symbols referenced:
  - [TypeCacheEntry](../T/TypeCacheEntry.md)

- Called from (representative examples):
  - [record_cmp](../r/record_cmp.md) (src/backend/utils/adt/rowtypes.c:882)
  - [record_eq](../r/record_eq.md) (src/backend/utils/adt/rowtypes.c:1126)
  - [record_image_cmp](../r/record_image_cmp.md) (src/backend/utils/adt/rowtypes.c:1388)
  - [record_image_eq](../r/record_image_eq.md) (src/backend/utils/adt/rowtypes.c:1634)
  - [hash_record](../h/hash_record.md) (src/backend/utils/adt/rowtypes.c:1832)
  - [hash_record_extended](../h/hash_record_extended.md) (src/backend/utils/adt/rowtypes.c:1953)

## Notes and Other Information
- Serves as a thin wrapper around PostgreSQL's type cache system for comparison operations
- Used primarily in record comparison and hashing functions
- The single-member design reflects the comprehensive nature of TypeCacheEntry
- Part of PostgreSQL's optimization strategy for record comparison operations
- Located in src/backend/utils/adt/rowtypes.c at lines 54-57
- Utilized by RecordCompareData for multi-column record comparisons