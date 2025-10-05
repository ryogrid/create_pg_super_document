# lseg_gt

## Location
[src/backend/utils/adt/geo_ops.c:2276-2285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2276-L2285)

## Overview
Compares two line segments to determine if the length of the first segment is greater than the length of the second segment.

## Definition

```c
Datum
lseg_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "greater than" comparison operator for PostgreSQL's line segment (LSEG) data type. It calculates the lengths of both input line segments using the distance between their endpoints and compares them using floating-point comparison. The function returns true if the first line segment is longer than the second line segment.

The comparison is performed by:
1. Extracting two LSEG arguments from the function call
2. Computing the distance between endpoints for each segment using 
3. Comparing the distances using the  (floating-point greater than) function
4. Returning the boolean result

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Pointer to first line segment (LSEG)
  - Second argument: Pointer to second line segment (LSEG)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts LSEG argument from function call
  - : Calculates distance between two points
  - : Floating-point greater than comparison
  - : Returns boolean result
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operators
- Uses floating-point arithmetic for length calculations, so standard floating-point precision considerations apply
- The comparison is based on Euclidean distance between the segment endpoints
- Typically used in SQL queries with the '>' operator between LSEG values
- Complementary to the  function for opposite direction comparison

## Simplified Source

```c
Datum lseg_gt(PG_FUNCTION_ARGS) {
    // Extract two line segments from function arguments
    LSEG *l1 = PG_GETARG_LSEG_P(0);
    LSEG *l2 = PG_GETARG_LSEG_P(1);

    // Compare lengths: l1 > l2 if length of l1 is greater than length of l2
    PG_RETURN_BOOL(FPgt(point_dt(&l1->p[0], &l1->p[1]),
                        point_dt(&l2->p[0], &l2->p[1])));
}
```