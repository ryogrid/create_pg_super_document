# float8_var_samp

## Location
src/backend/utils/adt/float.c: 3160 - 3181

## Overview
Final function for the VAR_SAMP aggregate that computes the sample variance from accumulated transition state values using Bessel's correction.

## Definition


## Detailed Description
The  function serves as the final function for PostgreSQL's VAR_SAMP (sample variance) aggregate when operating on floating-point data. It computes the sample variance by dividing the sum of squared deviations (Sxx) by (N-1), where N is the count of values. This implements Bessel's correction, which provides an unbiased estimator of the population variance from a sample.

Sample variance uses the formula: s² = Σ(xi - x̄)² / (N-1), which differs from population variance that divides by N. The (N-1) divisor corrects for the bias that occurs when estimating population variance from a sample, since the sample mean is used instead of the true population mean.

The function returns NULL for datasets with 0 or 1 values (N ≤ 1) since sample variance is mathematically undefined in these cases - you need at least 2 data points to compute a meaningful sample variance. For valid datasets, it returns Sxx/(N-1).

## Parameters / Member Variables
-  (ArrayType*): Transition state array containing [N, Sx, Sxx] where N (count) and Sxx (sum of squared deviations) are used for sample variance calculation

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and extracts float8 values from the transition array
  - : PostgreSQL macro to get array argument
  - : PostgreSQL macro to return NULL value
  - : PostgreSQL macro to return float8 value

- Called from (representative examples):
  - VAR_SAMP aggregate functions operating on floating-point columns
  - VARIANCE aggregate (which is an alias for VAR_SAMP in PostgreSQL)
  - Statistical computations requiring sample variance

## Notes and Other Information
- Returns NULL for datasets with N ≤ 1 since sample variance requires at least 2 values
- Uses Bessel's correction (divides by N-1) to provide unbiased population variance estimation
- Uses first and third elements of transition array (N and Sxx), ignores Sx (sum)
- [Result](../R/Result.md) is guaranteed to be non-negative due to mathematical properties of variance
- Implements unbiased sample variance, unlike VAR_POP which computes biased population variance
- Part of PostgreSQL's statistical aggregate infrastructure  
- Located in src/backend/utils/adt/float.c:3160-3181
- Relies on numerically stable Sxx values computed by accumulator functions