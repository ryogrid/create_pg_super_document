# get_range_collation

## Location
[src/backend/utils/cache/lsyscache.c:3433-3457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3433-L3457)

## Overview
Returns the collation OID of a given PostgreSQL range type.

## Definition
```c
Oid get_range_collation(Oid rangeOid)
```

## Detailed Description
The get_range_collation function retrieves the collation OID associated with a PostgreSQL range type from the pg_range system catalog. Collations define the sorting and comparison rules for text-based data types. For range types, the collation determines how the range bounds are compared and ordered. This function is particularly important for range types whose subtype is collatable (such as text-based ranges). The collation information is essential for proper ordering and comparison operations on range values.

## Parameters / Member Variables
- `rangeOid`: The OID (Object Identifier) of the range type whose collation is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup using RANGETYPE cache)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache reference cleanup)
  - Form_pg_range (structure type for pg_range catalog)
- Called from (representative examples):
  - [CheckAttributeType](../C/CheckAttributeType.md)

## Notes and Other Information
- Returns InvalidOid if the provided OID does not correspond to a range type
- Also returns InvalidOid if the range type's subtype is not collatable
- Part of the PG_RANGE CACHES section in lsyscache.c
- The collation information is stored in the rngcollation field of the pg_range system catalog
- Essential for proper comparison and ordering operations on range types with collatable subtypes
- Used primarily during table creation and type checking to ensure collation consistency
- Critical for range types based on text, varchar, char, and other collatable data types
- Enables proper sorting and comparison behavior for ranges containing textual data