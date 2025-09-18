# brin_minmax_multi_distance_float8

## Location
src/backend/access/brin/brin_minmax_multi.c: 1909 - 1934

## Overview
Computes the distance between two float8 values used as range boundaries in BRIN minmax-multi indexes, handling NaN values appropriately.

## Definition


## Detailed Description
This function calculates the distance between two double-precision floating-point values for BRIN (Block Range Index) minmax-multi operator class. It performs plain subtraction to determine the range size, with special handling for NaN (Not-a-Number) values. The function is designed specifically for range boundaries where the first argument should be less than or equal to the second argument.

The function implements PostgreSQL's distance semantics for floating-point ranges:
- If both values are NaN, they are considered identical (distance = 0)
- If only one value is NaN, infinite distance is returned
- For normal values, it returns the simple difference (a2 - a1)

This distance computation is crucial for BRIN minmax-multi indexes to determine how to merge and split ranges efficiently.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: First float8 value (range minimum)
  - Argument 1: Second float8 value (range maximum)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts float8 arguments from function call
  - : Checks if a floating-point value is NaN
  - : Returns positive infinity as float8
  - : Returns float8 result from PostgreSQL function
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes that a1 <= a2 (enforced by Assert)
- Special NaN handling follows IEEE 754 semantics adapted for PostgreSQL's needs
- Used internally by BRIN minmax-multi operator class for float8 data types
- Part of the extensible operator class framework for BRIN indexes
- The distance calculation is essential for determining when ranges should be merged or split in multi-range BRIN summaries