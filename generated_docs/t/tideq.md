# tideq

## Location
[src/backend/utils/adt/tid.c:176-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L176-L184)

## Overview
The `tideq` function implements the equality comparison operator for PostgreSQL's TID (tuple identifier) data type, determining if two TID values are equal.

## Definition
```c
Datum tideq(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tideq` function compares two ItemPointer structures for equality and returns a boolean result. It serves as the implementation of the "=" operator for TID data types in PostgreSQL's SQL operations. The function uses the lower-level `ItemPointerCompare` function to perform the actual comparison and checks if the result is zero (indicating equality). This function is part of the public API for TID operations and is used by the PostgreSQL query executor when TID equality comparisons are needed in SQL queries.

## Parameters / Member Variables
- Input parameters:
  - First parameter accessed via `PG_GETARG_ITEMPOINTER(0)`: First TID value to compare
  - Second parameter accessed via `PG_GETARG_ITEMPOINTER(1)`: Second TID value to compare
- Internal variables:
  - `arg1`: Pointer to the first ItemPointer structure
  - `arg2`: Pointer to the second ItemPointer structure

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerCompare](../I/ItemPointerCompare.md): Low-level function that compares two ItemPointer structures
  - `PG_RETURN_BOOL`: PostgreSQL macro to return boolean datum
- Called from (representative examples):
  - PostgreSQL query executor when processing TID equality conditions in WHERE clauses
  - SQL operations involving TID comparisons (e.g., "WHERE ctid = '(0,1)'")
  - Index operations and constraint checking involving TID values
  - [Hash](../H/Hash.md) table lookups using TID values as keys

## Notes and Other Information
- The function returns true only when both the block number and offset number of both TIDs are identical
- Part of PostgreSQL's operator system for the TID data type
- The comparison is exact and does not perform any normalization or special handling
- The function assumes both input ItemPointer structures are valid
- This is one of several comparison operators available for TID values (others may include <, >, <=, >=, <>)
- The implementation is straightforward, delegating the actual comparison logic to `ItemPointerCompare`

## Simplified Source

```c
Datum tideq(PG_FUNCTION_ARGS) {
    ItemPointer tid1 = PG_GETARG_ITEMPOINTER(0);
    ItemPointer tid2 = PG_GETARG_ITEMPOINTER(1);

    // Return true if TIDs are equal (comparison result == 0)
    PG_RETURN_BOOL(ItemPointerCompare(tid1, tid2) == 0);
}
```