# record_image_ge

## Location
[src/backend/utils/adt/rowtypes.c:1777-1782](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1777-L1782)

## Overview
A PostgreSQL function that performs a "greater than or equal to" comparison between two record (composite) types using byte-oriented comparison rather than semantic equality.

## Definition

```c
Datum
record_image_ge(PG_FUNCTION_ARGS)
```
## Detailed Description
The `record_image_ge` function is a simple wrapper around `record_image_cmp` that returns true if the first record is "greater than or equal to" the second record according to byte-oriented comparison. This function implements the "image" comparison semantics where different representations of values that are considered semantically equal are treated as distinct. For example, with citext type, 'A' and 'a' are equal semantically but not identical in byte representation.

The function takes two HeapTupleHeader arguments through the PostgreSQL function call interface and returns a boolean Datum indicating whether the first record is greater than or equal to the second.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function call information containing two HeapTupleHeader arguments representing the records to compare

## Dependencies
- Functions called/Symbols referenced:
  - [record_image_cmp](record_image_cmp.md) - Internal byte-oriented comparison function that performs the actual comparison
  - `PG_RETURN_BOOL` - PostgreSQL macro for returning boolean values
  - `PG_FUNCTION_ARGS` - PostgreSQL function call interface

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's record/composite type comparison operators
- Uses "image" comparison which compares byte representations rather than semantic values
- Returns true if record_image_cmp returns zero or positive value (>= 0)
- Located in src/backend/utils/adt/rowtypes.c:1777-1782
- Part of a family of image comparison functions (lt, gt, le, ge) that all delegate to record_image_cmp

## Simplified Source

```c
Datum record_image_ge(PG_FUNCTION_ARGS) {
    // Return true if first record is greater than or equal to second record
    return (record_image_cmp(fcinfo) >= 0);
}
```