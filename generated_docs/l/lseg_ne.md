# lseg_ne

## Location
[src/backend/utils/adt/geo_ops.c:2246-2255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2246-L2255)

## Overview
Determines if two line segments are not equal by comparing their corresponding endpoints.

## Definition

```c
Datum
lseg_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function tests whether two line segments are not equal. Two line segments are considered not equal if either their first endpoints are not equal OR their second endpoints are not equal. This is the logical negation of the  function. The function returns true if at least one pair of corresponding endpoints differs between the two segments.

## Parameters / Member Variables
- : First line segment (LSEG type) obtained via 
  - : First point of the first segment
  - : Second point of the first segment
- : Second line segment (LSEG type) obtained via 
  - : First point of the second segment
  - : Second point of the second segment

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract LSEG arguments from function call
  - : Function to compare two points for equality
  - : Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns a boolean Datum indicating whether the segments are not equal
- Uses point-to-point comparison with logical negation and OR operation
- Part of PostgreSQL's geometric data type operations for line segments
- Complementary to  function - returns the opposite result
- The comparison uses OR logic: segments are not equal if ANY corresponding endpoints differ
- Located in geo_ops.c alongside other geometric utility functions
- More efficient than negating the result of  due to short-circuit evaluation

## Simplified Source

```c
Datum lseg_ne(PG_FUNCTION_ARGS) {
    // Extract two line segments from function arguments
    LSEG *l1 = PG_GETARG_LSEG_P(0);
    LSEG *l2 = PG_GETARG_LSEG_P(1);

    // Two segments are not equal if any endpoint differs
    PG_RETURN_BOOL(!point_eq_point(&l1->p[0], &l2->p[0]) ||
                   !point_eq_point(&l1->p[1], &l2->p[1]));
}
```