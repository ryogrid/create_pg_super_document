# lseg_ne

## Location
src/backend/utils/adt/geo_ops.c: 2246 - 2255

## Overview
Determines if two line segments are not equal by comparing their corresponding endpoints.

## Definition


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