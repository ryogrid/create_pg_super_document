# get_mincount_for_mcv_list

## Location
src/backend/statistics/mcv.c: 148 - 179

## Overview
Calculates the minimum number of times a value needs to appear in a sample for it to be included in the Most Common Values (MCV) list, based on statistical error analysis principles.

## Definition
```c
static double get_mincount_for_mcv_list(int samplerows, double totalrows)
```

## Detailed Description
This function implements a statistical threshold calculation for MCV list construction. It determines the minimum occurrence count required for a value to be statistically significant enough to include in the MCV list. The calculation is based on hypergeometric distribution theory and aims to keep the relative standard error below 20% to ensure reasonably accurate planner estimates.

The function uses the formula:
`cnt > n*(N-n) / (N-n+0.04*n*(N-1))`

Where:
- n = sample size (samplerows)
- N = total population size (totalrows)
- The 0.04 factor comes from the 20% relative error bound (0.2²)

The mathematical foundation assumes sampling without replacement and applies finite population correction. The 20% relative error bound is empirically determined to work well in practice.

## Parameters / Member Variables
- `samplerows`: The number of rows in the statistical sample (n)
- `totalrows`: The total number of rows in the table (N)

## Dependencies
- Functions called/Symbols referenced:
  - MCVList (referenced in context)
- Called from (representative examples):
  - statext_mcv_build

## Notes and Other Information
- Returns 0.0 if division by zero would occur (when n = N = 1)
- The bound is at most 25 and approaches 0 as sample size approaches 0 or total size
- The 10-instance rule of thumb for normal approximation of hypergeometric distribution underlies this calculation
- Designed to work with minimum sample sizes of 300 rows in practice
- The formula remains valid even when the result is less than 10, contrary to the strict statistical derivation