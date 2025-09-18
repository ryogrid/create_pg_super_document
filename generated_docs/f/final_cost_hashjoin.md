final_cost_hashjoin

## Overview
Computes the final cost estimate and result size of a hash join path, including detailed CPU costs for hash qualification, bucket scanning, and join processing.

## Definition
```c
void final_cost_hashjoin(PlannerInfo *root, HashPath *path,
                        JoinCostWorkspace *workspace,
                        JoinPathExtraData *extra)
```

## Detailed Description
This function completes the hash join cost estimation begun by initial_cost_hashjoin, adding the detailed CPU costs that were deferred in the preliminary estimate. It performs sophisticated analysis of hash bucket distribution and qualification costs.

Key aspects of the cost calculation include:

1. **Hash Bucket Analysis**: Estimates bucket size fractions and most common value (MCV) frequencies for each hash clause, using cached statistics from RestrictInfo nodes to avoid repeated calculations.

2. **Join Type Optimization**: For SEMI/ANTI joins and unique inner relations, models early termination behavior where scanning stops after the first match, significantly reducing expected costs.

3. **Memory Pressure Handling**: Applies disable_cost penalty when the MCV bucket would exceed hash_mem limits, since the executor cannot split equal values across batches.

4. **Qualification Cost Modeling**: Distinguishes between hash equality checks and additional join restrictions, applying a 50% reduction factor to hash qualification costs since quals are only evaluated when hash codes match exactly.

The function uses the most conservative (smallest) bucket size estimates across all hash clauses and handles both sides of hash clauses correctly by determining which side corresponds to the inner relation.

## Parameters / Member Variables
- `root`: PlannerInfo containing query planning context and statistics
- `path`: HashPath being costed (updated with final costs and batch estimates)
- `workspace`: JoinCostWorkspace from initial_cost_hashjoin with preliminary estimates
- `extra`: JoinPathExtraData with join information including semifactors for SEMI/ANTI joins

## Dependencies
- Functions called/Symbols referenced:
  - get_parallel_divisor
  - clamp_row_est
  - bms_is_subset
  - estimate_hash_bucket_stats
  - get_rightop
  - get_leftop
  - relation_byte_size
  - get_hash_memory_limit
  - cost_qual_eval
  - approx_tuple_count
- Called from (representative examples):
  - create_hashjoin_path

## Notes and Other Information
- Caches bucket size and MCV frequency estimates in RestrictInfo nodes for reuse across similar hash join paths
- Sets path->num_batches and path->inner_rows_total for use by the executor
- For unique inner paths, assumes perfect hash distribution (bucketsize = 1/virtualbuckets, mcvfreq = 0)
- Uses semifactors to model early termination in SEMI/ANTI joins with sophisticated outer_matched_rows calculations
- Applies different cost models for matched vs unmatched outer rows in SEMI/ANTI joins
- For parallel paths, scales row estimates using parallel divisor before final cost calculation
- Adds disable_cost penalty if enable_hashjoin is disabled