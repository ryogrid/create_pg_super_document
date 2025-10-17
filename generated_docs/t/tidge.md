# tidge

## Location
[src/backend/utils/adt/tid.c:221-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L221-L229)

## Overview
tidge is a PostgreSQL function that performs greater-than-or-equal comparison between two tuple identifiers (ItemPointer), returning true if the first is greater than or equal to the second.

## Definition
```c
Datum tidge(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "greater than or equal" operator for the tid (tuple identifier) data type in PostgreSQL. It takes two ItemPointer arguments and compares them using ItemPointerCompare, returning true if the first pointer is greater than or equal to the second (comparison result >= 0). This operator is essential for range queries and inclusive comparisons on tuple identifiers with upper bounds.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `arg1`: First ItemPointer (tuple identifier) to compare
  - `arg2`: Second ItemPointer (tuple identifier) to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (extracts ItemPointer from function arguments)
  - [ItemPointerCompare](../I/ItemPointerCompare.md) (performs the actual comparison between ItemPointers)
  - PG_RETURN_BOOL (returns boolean result)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/tid.c:221-229
- This is part of the family of tid comparison operators (tideq, tidne, tidlt, tidle, tidgt, tidge)
- Uses ItemPointerCompare result >= 0 to determine if first argument is greater than or equal to second
- Essential for range queries with inclusive upper bounds involving tuple identifiers

## Simplified Source

```c
Datum tidge(PG_FUNCTION_ARGS) {
    ItemPointer tid1 = PG_GETARG_ITEMPOINTER(0);
    ItemPointer tid2 = PG_GETARG_ITEMPOINTER(1);

    // Return true if first TID is greater than or equal to second (comparison result >= 0)
    PG_RETURN_BOOL(ItemPointerCompare(tid1, tid2) >= 0);
}
```