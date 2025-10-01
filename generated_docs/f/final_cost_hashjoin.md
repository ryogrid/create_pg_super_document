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
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [estimate_hash_bucket_stats](../e/estimate_hash_bucket_stats.md)
  - [get_rightop](../g/get_rightop.md)
  - [get_leftop](../g/get_leftop.md)
  - [relation_byte_size](../r/relation_byte_size.md)
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - [approx_tuple_count](../a/approx_tuple_count.md)
- Called from (representative examples):
  - [create_hashjoin_path](../c/create_hashjoin_path.md)

## Notes and Other Information
- Caches bucket size and MCV frequency estimates in RestrictInfo nodes for reuse across similar hash join paths
- Sets path->num_batches and path->inner_rows_total for use by the executor
- For unique inner paths, assumes perfect hash distribution (bucketsize = 1/virtualbuckets, mcvfreq = 0)
- Uses semifactors to model early termination in SEMI/ANTI joins with sophisticated outer_matched_rows calculations
- Applies different cost models for matched vs unmatched outer rows in SEMI/ANTI joins
- For parallel paths, scales row estimates using parallel divisor before final cost calculation
- Adds disable_cost penalty if enable_hashjoin is disabled

## Simplified Source

```c
void
final_cost_hashjoin(PlannerInfo *root, HashPath *path,
                   JoinCostWorkspace *workspace, JoinPathExtraData *extra)
{
    Path *outer_path = path->jpath.outerjoinpath;
    Path *inner_path = path->jpath.innerjoinpath;
    double outer_path_rows = outer_path->rows;
    double inner_path_rows = inner_path->rows;
    Cost startup_cost = workspace->startup_cost;
    Cost run_cost = workspace->run_cost;
    double virtualbuckets = workspace->numbuckets * workspace->numbatches;

    // Set basic path properties
    path->jpath.path.rows = (path->jpath.path.param_info) ?
        path->jpath.path.param_info->ppi_rows : path->jpath.path.parent->rows;

    // Handle parallel execution scaling
    if (path->jpath.path.parallel_workers > 0)
        path->jpath.path.rows /= get_parallel_divisor(&path->jpath.path);

    // Apply disable cost if hash joins are disabled
    if (!enable_hashjoin)
        startup_cost += disable_cost;

    // Determine hash bucket statistics for inner relation
    Selectivity innerbucketsize, innermcvfreq;
    if (IsA(inner_path, UniquePath))
    {
        // Unique paths have perfect distribution
        innerbucketsize = 1.0 / virtualbuckets;
        innermcvfreq = 0.0;
    }
    else
    {
        // Find smallest bucket size across all hash clauses
        innerbucketsize = 1.0;
        innermcvfreq = 1.0;
        foreach_hash_clause()
        {
            estimate_bucket_stats_and_cache();
            innerbucketsize = Min(innerbucketsize, this_bucketsize);
            innermcvfreq = Min(innermcvfreq, this_mcvfreq);
        }
    }

    // Apply memory pressure penalty if MCV bucket exceeds hash_mem
    if (mcv_bucket_size > get_hash_memory_limit())
        startup_cost += disable_cost;

    // Calculate join costs based on join type
    QualCost hash_qual_cost, qp_qual_cost;
    cost_qual_eval(&hash_qual_cost, path->path_hashclauses, root);
    cost_qual_eval(&qp_qual_cost, path->jpath.joinrestrictinfo, root);

    double hashjointuples;
    if (is_semi_anti_or_unique_join())
    {
        // SEMI/ANTI joins stop at first match
        double outer_matched_rows = outer_path_rows * extra->semifactors.outer_match_frac;
        double inner_scan_frac = 2.0 / (extra->semifactors.match_count + 1.0);

        // Cost for matched rows (early termination)
        run_cost += hash_qual_cost.per_tuple * outer_matched_rows *
                   inner_path_rows * innerbucketsize * inner_scan_frac * 0.5;

        // Cost for unmatched rows (average bucket scan)
        run_cost += hash_qual_cost.per_tuple * (outer_path_rows - outer_matched_rows) *
                   (inner_path_rows / virtualbuckets) * 0.05;

        hashjointuples = (path->jpath.jointype == JOIN_ANTI) ?
            outer_path_rows - outer_matched_rows : outer_matched_rows;
    }
    else
    {
        // Regular joins scan full buckets
        run_cost += hash_qual_cost.per_tuple * outer_path_rows *
                   inner_path_rows * innerbucketsize * 0.5;
        hashjointuples = approx_tuple_count(root, &path->jpath, hashclauses);
    }

    // Add costs for additional quals and target list evaluation
    run_cost += (cpu_tuple_cost + qp_qual_cost.per_tuple) * hashjointuples;
    run_cost += path->jpath.path.pathtarget->cost.per_tuple * path->jpath.path.rows;

    // Set final costs
    path->jpath.path.startup_cost = startup_cost;
    path->jpath.path.total_cost = startup_cost + run_cost;
}
```