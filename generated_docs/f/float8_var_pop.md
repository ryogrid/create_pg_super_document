# float8_var_pop

## Location
[src/backend/utils/adt/float.c:3138-3159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3138-L3159)

## Overview
Final function for the VAR_POP aggregate that computes the population variance from accumulated transition state values.

## Definition


## Detailed Description
The  function serves as the final function for PostgreSQL's VAR_POP (population variance) aggregate when operating on floating-point data. It takes the transition state array produced by accumulator functions and computes the population variance by dividing the sum of squared deviations (Sxx) by the count (N).

Population variance uses the formula: σ² = Σ(xi - μ)² / N, where N is the total number of values. This differs from sample variance which divides by (N-1). The function leverages the numerically stable sum of squared deviations maintained by the accumulator functions through the Youngs-Cramer algorithm.

The function returns NULL for empty input sets (N == 0) since population variance is mathematically undefined for zero elements. For non-empty sets, it returns Sxx/N, which is guaranteed to be non-negative due to the mathematical properties of variance.

## Parameters / Member Variables
-  (ArrayType*): Transition state array containing [N, Sx, Sxx] where N (count) and Sxx (sum of squared deviations) are used for variance calculation

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and extracts float8 values from the transition array
  - : PostgreSQL macro to get array argument
  - : PostgreSQL macro to return NULL value
  - : PostgreSQL macro to return float8 value

- Called from (representative examples):
  - VAR_POP aggregate functions operating on floating-point columns
  - Statistical computations requiring population variance

## Notes and Other Information
- Returns NULL for empty input sets since population variance is undefined for N=0
- Uses first and third elements of transition array (N and Sxx), ignores Sx (sum)
- [Result](../R/Result.md) is guaranteed to be non-negative due to mathematical properties of variance
- Population variance divides by N, unlike sample variance which divides by (N-1)
- Part of PostgreSQL's statistical aggregate infrastructure
- Located in src/backend/utils/adt/float.c:3138-3159
- Relies on numerically stable Sxx values computed by accumulator functions