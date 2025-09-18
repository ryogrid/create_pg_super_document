# box_eq

## Location
[src/backend/utils/adt/geo_ops.c:762-770](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L762-L770)

## Overview
The  function checks if two PostgreSQL BOX geometries are equal by comparing their areas.

## Definition


## Detailed Description
This function implements the equality operator for BOX data types in PostgreSQL. It determines equality between two boxes by comparing their areas using floating-point equality comparison. Two boxes are considered equal if they have the same area, regardless of their position or orientation. The function uses the internal  function to calculate the area of each box and then compares them using PostgreSQL's floating-point equality function .

## Parameters / Member Variables
- : PostgreSQL function call convention containing:
  - First argument (index 0): Pointer to first BOX structure
  - Second argument (index 1): Pointer to second BOX structure

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts BOX pointer from function arguments
  - : Calculates the area of a BOX
  - : Floating-point equality comparison function
  - : Returns boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator system)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's SQL equality operator (=) for BOX types
- The equality comparison is based solely on area, not on position or exact coordinate matching
- Uses floating-point arithmetic, so standard floating-point precision considerations apply
- Located in src/backend/utils/adt/geo_ops.c:762-770