# float4_accum

## Location
src/backend/utils/adt/float.c: 3033 - 3117

## Overview
Accumulator function for statistical aggregates on float4 (single precision) values that maintains running statistics using double precision arithmetic internally for improved numerical stability.

## Definition


## Detailed Description
The  function is a transition function used by PostgreSQL's statistical aggregate functions (like AVG, VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP) when operating on float4 data. It implements the Youngs-Cramer algorithm to maintain numerically stable running statistics by tracking three values: count (N), sum (Sx), and sum of squared deviations (Sxx).

The function converts the input float4 value to float8 precision for internal calculations to minimize rounding errors that would accumulate with repeated float4 arithmetic. It handles special cases like infinity and NaN values appropriately, ensuring that statistical computations remain mathematically sound even with edge case inputs.

The function optimizes memory usage when called in an aggregate context by modifying the transition array in-place rather than creating new arrays for each accumulation step.

## Parameters / Member Variables
-  (ArrayType*): Input transition state array containing [N, Sx, Sxx] where N is count, Sx is sum, and Sxx is sum of squared deviations
-  (float4): New float4 value to incorporate into the running statistics, converted to float8 for calculations

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and extracts float8 values from the transition array
  - : Determines if function is called in aggregate context for optimization
  - : Reports overflow errors when finite inputs produce infinite results
  - : Returns NaN value for float8 type
  - : Creates new array when not in aggregate context
  - Various PostgreSQL macros: , , , 

- Called from (representative examples):
  - Statistical aggregate functions operating on float4 columns
  - AVG, VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP aggregates

## Notes and Other Information
- Uses Youngs-Cramer algorithm for numerically stable computation of variance
- Converts float4 inputs to float8 internally to reduce accumulation of rounding errors
- Handles special IEEE 754 values (infinity, NaN) correctly
- Implements overflow detection for finite inputs that produce infinite intermediate results
- Optimized for aggregate context by modifying arrays in-place when possible
- Part of PostgreSQL's statistical function infrastructure in src/backend/utils/adt/float.c:3033-3117