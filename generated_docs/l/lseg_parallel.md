# lseg_parallel

## Location
[src/backend/utils/adt/geo_ops.c:2198-2209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2198-L2209)

## Overview
Determines whether two line segments are parallel by comparing their slopes as a PostgreSQL function.

## Definition
```c
Datum lseg_parallel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `lseg_parallel` function is a PostgreSQL built-in function that tests whether two line segments are parallel. It extracts two line segments from the function arguments and compares their slopes using the `lseg_sl` function for each segment. Two line segments are considered parallel if they have equal slopes (within floating-point precision tolerance). The function uses `FPeq` for floating-point equality comparison to handle potential precision issues. The result is returned as a PostgreSQL boolean value.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - First argument: Line segment l1 (retrieved using PG_GETARG_LSEG_P(0))
  - Second argument: Line segment l2 (retrieved using PG_GETARG_LSEG_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSEG_P: PostgreSQL macro to extract line segment arguments
  - [lseg_sl](lseg_sl.md): Calculates slope of a line segment
  - [FPeq](../F/FPeq.md): Floating-point equality comparison with tolerance
  - PG_RETURN_BOOL: PostgreSQL macro to return boolean result
  - [LSEG](../L/LSEG.md): Line segment data structure type

- Called from (representative examples):
  - This function is typically called from SQL queries rather than C code
  - No direct C references found in the current codebase

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL as lseg_parallel()
- Uses floating-point equality comparison (FPeq) to handle precision issues inherent in slope calculations
- Two line segments are parallel if they have the same slope, regardless of their position
- Handles special cases like vertical lines (infinite slope) correctly through the underlying slope calculation
- Part of PostgreSQL's geometric data type support for spatial analysis
- Useful for geometric queries involving parallel line detection in spatial databases
- The parallelism test is based purely on slope equality and does not consider whether the segments are collinear