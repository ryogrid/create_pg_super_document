# tidne

## Location
src/backend/utils/adt/tid.c: 185 - 193

## Overview
tidne is a PostgreSQL function that performs inequality comparison between two tuple identifiers (ItemPointer), returning true if they are not equal.

## Definition


## Detailed Description
This function implements the "not equals" operator for the tid (tuple identifier) data type in PostgreSQL. It takes two ItemPointer arguments and compares them using ItemPointerCompare, returning true if the pointers are not equal (comparison result != 0). This is part of PostgreSQL's tuple identifier comparison operators, which are essential for identifying specific rows within tables.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - : First ItemPointer (tuple identifier) to compare
  - : Second ItemPointer (tuple identifier) to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (extracts ItemPointer from function arguments)
  - [ItemPointerCompare](../I/ItemPointerCompare.md) (performs the actual comparison between ItemPointers)
  - PG_RETURN_BOOL (returns boolean result)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/tid.c:185-193
- This is part of the family of tid comparison operators (tideq, tidne, tidlt, tidle, tidgt, tidge)
- Returns the logical opposite of tideq (tid equals) function
- Essential for SQL WHERE clauses and other conditional operations involving tuple identifiers