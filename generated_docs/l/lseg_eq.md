# lseg_eq

## Location
[src/backend/utils/adt/geo_ops.c:2236-2245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2236-L2245)

## Overview
Determines if two line segments are equal by comparing their corresponding endpoints.

## Definition

```c
Datum
lseg_eq(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function tests whether two line segments are equal. Two line segments are considered equal if their corresponding endpoints are equal. The function compares the first point of the first segment with the first point of the second segment, and the second point of the first segment with the second point of the second segment. Both point comparisons must be true for the segments to be considered equal.

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
- Returns a boolean Datum indicating whether the segments are equal
- Uses point-to-point comparison which handles floating-point precision issues
- Part of PostgreSQL's geometric data type operations for line segments
- The comparison is order-dependent: the first point of l1 must equal the first point of l2, etc.
- Located in geo_ops.c alongside other geometric utility functions
- Does not consider segments equal if they have the same endpoints in reverse order