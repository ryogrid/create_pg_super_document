# tidlt

## Location
src/backend/utils/adt/tid.c: 194 - 202

## Overview
tidlt is a PostgreSQL function that performs less-than comparison between two tuple identifiers (ItemPointer), returning true if the first is less than the second.

## Definition
```c
Datum tidlt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "less than" operator for the tid (tuple identifier) data type in PostgreSQL. It takes two ItemPointer arguments and compares them using ItemPointerCompare, returning true if the first pointer is less than the second (comparison result < 0). This enables ordering operations and range queries on tuple identifiers.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `arg1`: First ItemPointer (tuple identifier) to compare
  - `arg2`: Second ItemPointer (tuple identifier) to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (extracts ItemPointer from function arguments)
  - ItemPointerCompare (performs the actual comparison between ItemPointers)
  - PG_RETURN_BOOL (returns boolean result)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/tid.c:194-202
- This is part of the family of tid comparison operators (tideq, tidne, tidlt, tidle, tidgt, tidge)
- Uses ItemPointerCompare result < 0 to determine if first argument is less than second
- Essential for ORDER BY clauses and other sorting operations involving tuple identifiers