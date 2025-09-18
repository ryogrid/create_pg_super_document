# float8_avg

## Location
src/backend/utils/adt/float.c: 3118 - 3137

## Overview
Final function for the AVG aggregate that computes the arithmetic mean from accumulated transition state values maintained by accumulator functions.

## Definition


## Detailed Description
The  function serves as the final function for PostgreSQL's AVG aggregate when operating on floating-point data. It takes the transition state array produced by accumulator functions (like  or ) and computes the final average by dividing the sum (Sx) by the count (N).

The function implements SQL standard behavior by returning NULL when no input values were provided (N == 0), which is the correct result for AVG over an empty set according to SQL specifications. For non-empty sets, it returns the arithmetic mean as Sx/N.

The function expects a 3-element transition array but only uses the first two elements: the count and sum. The third element (sum of squared deviations) is ignored as it's not needed for average calculation.

## Parameters / Member Variables
-  (ArrayType*): Transition state array containing [N, Sx, Sxx] where only N (count) and Sx (sum) are used for average calculation

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and extracts float8 values from the transition array
  - : PostgreSQL macro to get array argument
  - : PostgreSQL macro to return NULL value
  - : PostgreSQL macro to return float8 value

- Called from (representative examples):
  - AVG aggregate functions operating on floating-point columns
  - Statistical computations requiring arithmetic mean

## Notes and Other Information
- Returns NULL for empty input sets per SQL standard
- Only uses first two elements of 3-element transition array (N and Sx)
- Simple division operation: result = sum / count
- Part of PostgreSQL's aggregate function infrastructure
- Located in src/backend/utils/adt/float.c:3118-3137
- Handles the final step of AVG aggregate computation after accumulation phase