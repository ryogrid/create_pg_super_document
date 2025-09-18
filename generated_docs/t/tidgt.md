# tidgt

## Location
src/backend/utils/adt/tid.c: 212 - 220

## Overview
tidgt is a PostgreSQL function that performs greater-than comparison between two tuple identifiers (ItemPointer), returning true if the first is greater than the second.

## Definition
```c
Datum tidgt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "greater than" operator for the tid (tuple identifier) data type in PostgreSQL. It takes two ItemPointer arguments and compares them using ItemPointerCompare, returning true if the first pointer is greater than the second (comparison result > 0). This enables reverse ordering operations and upper-bound range queries on tuple identifiers.

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
- Located in src/backend/utils/adt/tid.c:212-220
- This is part of the family of tid comparison operators (tideq, tidne, tidlt, tidle, tidgt, tidge)
- Uses ItemPointerCompare result > 0 to determine if first argument is greater than second
- Essential for descending ORDER BY clauses and other reverse sorting operations involving tuple identifiers