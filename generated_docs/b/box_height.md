# box_height

## Location
src/backend/utils/adt/geo_ops.c: 820 - 831

## Overview
Returns the height (vertical magnitude) of a geometric box data type in PostgreSQL.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that calculates and returns the height of a BOX geometric data type. It operates by extracting the BOX argument from the function call and delegating the actual height calculation to the internal helper function . The height is computed as the difference between the high and low y-coordinates of the box (vertical magnitude).

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  - BOX pointer: The box geometry whose height is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - : PostgreSQL macro to extract BOX pointer from function arguments
  - : Internal helper function that performs the actual height calculation
  - : PostgreSQL macro to return a float8 value
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations located in 
- The actual height calculation is performed by the static helper function , which computes 
- Returns a float8 (double precision) value representing the height
- Part of the geometric operations suite for the BOX data type in PostgreSQL