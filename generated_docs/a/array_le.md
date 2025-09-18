# array_le

## Location
src/backend/utils/adt/arrayfuncs.c: 3949 - 3954

## Overview
array_le is a PostgreSQL function that determines whether one array is less than or equal to another array based on lexicographic comparison.

## Definition
```c
Datum array_le(PG_FUNCTION_ARGS)
```

## Detailed Description
array_le is a comparison function that implements the less-than-or-equal-to operator (<=) for arrays in PostgreSQL. It serves as a thin wrapper around the internal array_cmp function, returning true if the first array argument is lexicographically less than or equal to the second array argument. The function follows PostgreSQLs standard calling convention for SQL-callable functions, using the PG_FUNCTION_ARGS macro for parameter handling and PG_RETURN_BOOL macro for return value.

The comparison is performed element-by-element using the appropriate comparison function for the arrays element type. If elements are equal, the comparison considers array dimensions and bounds to establish a total ordering.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS convention:
  - First argument: Array to compare (left operand)
  - Second argument: Array to compare against (right operand)
- Returns: Boolean datum indicating whether first array <= second array

## Dependencies
- Functions called/Symbols referenced:
  - [array_cmp](array_cmp.md) (internal comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - SQL queries using <= operator between arrays
  - B-tree index operations requiring array ordering

## Notes and Other Information
- Located in src/backend/utils/adt/arrayfuncs.c:3949-3954
- Part of PostgreSQLs array comparison operator family
- Requires arrays to have the same element type for comparison
- NULL handling follows PostgreSQL standards (NULLs are considered equal, NULL > non-NULL)
- Used internally by the query planner and executor for array comparisons
- Performance depends on array size and element type comparison complexity