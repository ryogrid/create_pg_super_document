# estimate_hash_bucket_stats

## Location
[src/backend/utils/adt/selfuncs.c:3811-3929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L3811-L3929)

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
  - [examine_variable](examine_variable.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - [get_variable_numdistinct](../g/get_variable_numdistinct.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - ReleaseVariableStats
- Called from:
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md) (in costsize.c:4277, 4295)

## Notes and Other Information
- The function includes extensive comments acknowledging its limitations in predicting how restriction clauses affect hash key distributions
- Bucketsize fraction is clamped to a sane range [1.0e-6, 1.0] to prevent extreme values
- The caller should verify that the mcv_freq doesn't indicate a single value that would create an impractically large bucket
- Uses PostgreSQL's statistics system (pg_statistic) to obtain MCV and ndistinct information
- Adjusts ndistinct based on estimated selectivity when relation statistics are available

## Simplified Source

```c
void estimate_hash_bucket_stats(PlannerInfo *root, Node *hashkey, double nbuckets,
                               Selectivity *mcv_freq,
                               Selectivity *bucketsize_frac) {
    VariableStatData vardata;
    double estfract, ndistinct, stanullfrac, avgfreq;
    bool isdefault;
    AttStatsSlot sslot;

    // Examine the hash key expression to get statistics
    examine_variable(root, hashkey, 0, &vardata);

    // Initialize MCV frequency to 0
    *mcv_freq = 0.0;

    // Get most common value frequency if available
    if (HeapTupleIsValid(vardata.statsTuple)) {
        if (get_attstatsslot(&sslot, vardata.statsTuple,
                            STATISTIC_KIND_MCV, InvalidOid,
                            ATTSTATSSLOT_NUMBERS)) {
            if (sslot.nnumbers > 0)
                *mcv_freq = sslot.numbers[0];  // First MCV stat
            free_attstatsslot(&sslot);
        }
    }

    // Get number of distinct values
    ndistinct = get_variable_numdistinct(&vardata, &isdefault);

    // If no real ndistinct available, use conservative estimate
    if (isdefault) {
        *bucketsize_frac = (Selectivity) Max(0.1, *mcv_freq);
        ReleaseVariableStats(vardata);
        return;
    }

    // Get null fraction and calculate average frequency
    stanullfrac = get_null_fraction(&vardata);
    avgfreq = (1.0 - stanullfrac) / ndistinct;

    // Adjust ndistinct for restriction clauses
    if (vardata.rel && vardata.rel->tuples > 0) {
        ndistinct *= vardata.rel->rows / vardata.rel->tuples;
        ndistinct = clamp_row_est(ndistinct);
    }

    // Calculate initial bucketsize estimate
    if (ndistinct > nbuckets)
        estfract = 1.0 / nbuckets;
    else
        estfract = 1.0 / ndistinct;

    // Adjust for skewed distribution using MCV frequency
    if (avgfreq > 0.0 && *mcv_freq > avgfreq)
        estfract *= *mcv_freq / avgfreq;

    // Clamp to reasonable range
    if (estfract < 1.0e-6)
        estfract = 1.0e-6;
    else if (estfract > 1.0)
        estfract = 1.0;

    *bucketsize_frac = (Selectivity) estfract;
    ReleaseVariableStats(vardata);
}
```