# estimate_hash_bucket_stats

## Location
src/backend/utils/adt/selfuncs.c: 3811 - 3929

## Overview
Estimates hash bucket statistics for a given expression when used as a hash key, determining the frequency of the most common value and the bucketsize fraction for hash join cost estimation.

## Definition

```c
void
estimate_hash_bucket_stats(PlannerInfo *root, Node *hashkey, double nbuckets,
						   Selectivity *mcv_freq,
						   Selectivity *bucketsize_frac)
```
## Detailed Description
This function estimates two critical statistics for hash join costing:

1. **Most Common Value Frequency**: The frequency of the most common value in the hash key expression
2. **Bucketsize Fraction**: The average number of entries per bucket divided by total tuples

The function assumes that the hash key distribution after applying restriction clauses will be similar to the underlying relation's distribution. It handles skewed distributions by adjusting the bucketsize fraction based on the most common value's frequency relative to the average frequency.

For perfect distribution with uniform bucket occupancy, the bucketsize fraction would be 1/nbuckets. However, real data distributions are often skewed, so the function estimates a "worst case" bucket size that's more representative of actual performance, particularly for the buckets that will be probed most frequently during joins.

When statistics are unavailable, it defaults to 0.1 to discourage hash joins on large, unknown distributions.

## Parameters
- : PlannerInfo structure containing query planning context
- : The expression node that will be used as the hash key
- : Number of hash buckets the executor will use
- : Output parameter for the most common value's frequency (set to 0.0 if unavailable)
- : Output parameter for the estimated bucketsize fraction

## Dependencies
- Functions called:
  - examine_variable
  - get_attstatsslot
  - free_attstatsslot
  - get_variable_numdistinct
  - clamp_row_est
  - ReleaseVariableStats
- Called from:
  - final_cost_hashjoin (in costsize.c:4277, 4295)

## Notes and Other Information
- The function includes extensive comments acknowledging its limitations in predicting how restriction clauses affect hash key distributions
- Bucketsize fraction is clamped to a sane range [1.0e-6, 1.0] to prevent extreme values
- The caller should verify that the mcv_freq doesn't indicate a single value that would create an impractically large bucket
- Uses PostgreSQL's statistics system (pg_statistic) to obtain MCV and ndistinct information
- Adjusts ndistinct based on estimated selectivity when relation statistics are available