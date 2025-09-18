# btarraycmp

## Location
src/backend/utils/adt/arrayfuncs.c: 3961 - 3972

## Overview
btarraycmp is a PostgreSQL function that provides a three-way comparison result for arrays, specifically designed for B-tree index operations.

## Definition
```c
Datum btarraycmp(PG_FUNCTION_ARGS)
```

## Detailed Description
btarraycmp is a comparison function that implements a three-way comparison for arrays in PostgreSQL, returning an integer result instead of a boolean. It serves as a thin wrapper around the internal array_cmp function, directly returning the comparison result (-1, 0, or 1) rather than converting it to a boolean. The function follows PostgreSQLs standard calling convention for SQL-callable functions, using the PG_FUNCTION_ARGS macro for parameter handling and PG_RETURN_INT32 macro for return value.

This function is specifically designed for B-tree index support operations where a three-way comparison result is needed to determine the relative ordering of array values for efficient indexing and sorting operations.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS convention:
  - First argument: Array to compare (left operand)
  - Second argument: Array to compare against (right operand)
- Returns: int32 datum with comparison result:
  - -1 if first array < second array
  - 0 if first array = second array  
  - 1 if first array > second array

## Dependencies
- Functions called/Symbols referenced:
  - array_cmp (internal comparison function)
  - PG_RETURN_INT32 (macro for returning integer result)
- Called from (representative examples):
  - B-tree index operations for array sorting
  - Internal PostgreSQL comparison routines requiring three-way results

## Notes and Other Information
- Located in src/backend/utils/adt/arrayfuncs.c:3961-3972
- Part of PostgreSQLs B-tree index support for arrays
- Requires arrays to have the same element type for comparison
- NULL handling follows PostgreSQL standards (NULLs are considered equal, NULL > non-NULL)
- Essential for efficient array indexing and sorting operations in PostgreSQL
- Performance depends on array size and element type comparison complexity
- Used internally by the B-tree access method for maintaining sorted array indexes