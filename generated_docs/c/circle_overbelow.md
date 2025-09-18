# circle_overbelow

## Location
[src/backend/utils/adt/geo_ops.c:4876-4888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4876-L4888)

## Overview
Tests whether the upper edge of one circle is at or below the upper edge of another circle.

## Definition


## Detailed Description
The  function performs a positional comparison between two circles to determine if the first circle's upper edge (center.y + radius) is at or below the second circle's upper edge. This is a geometric operator used in PostgreSQL's spatial data types for circle positioning queries. The function returns true if circle1's uppermost point has a y-coordinate less than or equal to circle2's uppermost point.

## Parameters / Member Variables
- : First circle argument obtained via 
- : Second circle argument obtained via 

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract CIRCLE argument from function call
  - : Floating-point less-than-or-equal comparison
  - : Floating-point addition function
  - : Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function implements the "&<|" operator for circle types in PostgreSQL
- Uses floating-point arithmetic with proper precision handling via  and 
- Located in 
- Part of PostgreSQL's geometric data type operators for spatial queries