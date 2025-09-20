# btrecordimagecmp

## Location
[src/backend/utils/adt/rowtypes.c:1783-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1783-L1793)

## Overview
A PostgreSQL B-tree comparison function for record (composite) types that performs byte-oriented comparison and returns a three-way comparison result for use in B-tree indexing operations.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
```
## Detailed Description
The `btrecordimagecmp` function is a wrapper around `record_image_cmp` that provides the three-way comparison interface required by PostgreSQL's B-tree indexing system. Unlike the boolean comparison functions (lt, gt, le, ge), this function returns an integer indicating the comparison result: negative if the first record is less than the second, zero if they are equal in byte representation, and positive if the first is greater than the second.

This function implements "image" comparison semantics where different representations of values that are considered semantically equal are treated as distinct. This is crucial for B-tree indexing where consistent ordering is required regardless of the logical equivalence of values.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function call information containing two HeapTupleHeader arguments representing the records to compare

## Dependencies
- Functions called/Symbols referenced:
  - [record_image_cmp](../r/record_image_cmp.md) - Internal byte-oriented comparison function that performs the actual comparison
  - `PG_RETURN_INT32` - PostgreSQL macro for returning 32-bit integer values
  - `PG_FUNCTION_ARGS` - PostgreSQL function call interface

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is designed specifically for B-tree indexing operations on record/composite types
- Returns a three-way comparison result: -1 (less than), 0 (equal), or 1 (greater than)
- Uses "image" comparison which compares byte representations rather than semantic values
- Located in src/backend/utils/adt/rowtypes.c:1783-1793
- Part of PostgreSQL's indexing infrastructure for composite types
- The "bt" prefix indicates this is a B-tree support function