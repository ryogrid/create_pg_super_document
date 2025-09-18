# analyze_mcv_list

## Location
src/backend/commands/analyze.c: 2934 - 3044

## Overview
Analyzes a list of most common values (MCVs) from a sample to determine how many are statistically significant enough to be stored in the table's MCV statistics.

## Definition
```c
static int analyze_mcv_list(int *mcv_counts, int num_mcv, double stadistinct, 
                           double stanullfrac, int samplerows, double totalrows)
```

## Detailed Description
The `analyze_mcv_list` function implements a statistical analysis algorithm to determine which most common values from a sample are significantly more frequent than they would be if they weren't tracked as MCVs. This is crucial for PostgreSQL's query planner to make accurate cardinality estimates.

The function uses a sophisticated statistical approach based on hypergeometric distribution and confidence intervals. It iteratively removes the least common values from the MCV list if they are not significantly more common than the estimated selectivity they would have as non-MCV values. The algorithm employs a continuity-corrected Wald-type confidence interval with approximately 2 standard errors to determine statistical significance.

The key insight is that all non-MCV values are assumed to be equally common after accounting for MCV frequencies and null values. The function calculates whether each MCV candidate is significantly more frequent than this baseline using hypergeometric variance calculations.

## Parameters / Member Variables
- `mcv_counts`: Array of counts for the most common values, ordered from most to least common
- `num_mcv`: Number of entries in the mcv_counts array
- `stadistinct`: Estimated number of distinct values (negative values indicate fraction of total rows)
- `stanullfrac`: Fraction of null values in the column
- `samplerows`: Number of rows in the analyzed sample
- `totalrows`: Total number of rows in the table

## Dependencies
- Functions called/Symbols referenced:
  - `sqrt` (mathematical function)
- Called from (representative examples):
  - `[compute_scalar_stats](../c/compute_scalar_stats.md)` (during ANALYZE operation)

## Notes and Other Information
The algorithm deliberately works by removing values from the full list rather than adding them, because the latter approach can fail when common values have similar frequencies and dominate the table. The function uses hypergeometric distribution calculations since sampling is done without replacement. If the entire table was sampled, all MCVs are kept. The statistical confidence interval uses a 2-standard-error threshold plus continuity correction of 0.5 to determine significance.