# cost_agg

## Location
[src/backend/optimizer/path/costsize.c:2650-2853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2650-L2853)

## Overview
Calculates the startup and total costs for performing an Agg plan node, considering different aggregation strategies (plain, sorted, hashed, mixed) and accounting for spilling to disk when memory limits are exceeded.

## Definition
void cost_agg(Path *path, PlannerInfo *root, AggStrategy aggstrategy, const AggClauseCosts *aggcosts, int numGroupCols, double numGroups, List *quals, Cost input_startup_cost, Cost input_total_cost, double input_tuples, double input_width)

## Detailed Description
The cost_agg function estimates the cost of executing an Agg node in PostgreSQL's query planner. Agg nodes perform aggregation operations like SUM, COUNT, AVG and handle grouping operations. The function supports multiple aggregation strategies:

- **AGG_PLAIN**: Single-group aggregation without grouping columns
- **AGG_SORTED**: Grouped aggregation on pre-sorted input, delivering output on-the-fly
- **AGG_HASHED**: Hash-based grouping that processes all input before producing output
- **AGG_MIXED**: Hybrid approach that may fall back to sorting if hashing is disabled

The costing model accounts for:
- Transition function costs (per input tuple) and finalization costs (per output group)
- Grouping comparison costs for sorted aggregation
- Hash computation and retrieval costs for hashed aggregation
- Disk spilling costs when hash aggregation exceeds memory limits
- HAVING clause evaluation costs and their selectivity impact

For hash aggregation spilling, the function performs sophisticated analysis using hash_agg_entry_size and hash_agg_set_limits to estimate memory usage, number of batches, and I/O costs including read/write penalties.

## Parameters / Member Variables
- : Path node to store the calculated costs and output row count
- : PlannerInfo structure containing planner context and aggregate transition info
- : Aggregation strategy (AGG_PLAIN, AGG_SORTED, AGG_HASHED, or AGG_MIXED)
- : Structure containing per-aggregate cost information, can be NULL for grouping-only operations
- : Number of columns used for grouping
- : Estimated number of output groups
- : List of HAVING clause expressions to evaluate
- : Startup cost from the input path
- : Total cost from the input path
- : Number of input tuples
- : Average width in bytes of input tuples

## Dependencies
- Functions called/Symbols referenced:
  - [hash_agg_entry_size](../h/hash_agg_entry_size.md) (estimates memory per hash table entry)
  - [hash_agg_set_limits](../h/hash_agg_set_limits.md) (calculates memory and group count limits)
  - [relation_byte_size](../r/relation_byte_size.md) (calculates total bytes for tuples)
  - [cost_qual_eval](cost_qual_eval.md) (evaluates HAVING clause costs)
  - [clamp_row_est](clamp_row_est.md) (ensures row estimates are within reasonable bounds)
  - [clauselist_selectivity](clauselist_selectivity.md) (calculates selectivity of HAVING clauses)
  - AggStrategy, AggClauseCosts (data types for aggregation parameters)
- Called from (representative examples):
  - [create_agg_path](create_agg_path.md) (in pathnode.c:3206)
  - [create_groupingsets_path](create_groupingsets_path.md) (in pathnode.c:3312, 3337, 3362)
  - [create_unique_path](create_unique_path.md) (in pathnode.c:1828)

## Notes and Other Information
- AGG_SORTED and AGG_HASHED are designed to have identical total CPU costs with different startup costs
- Uses dummy_aggcosts when aggcosts is NULL (typically for grouping-only hash aggregation)
- For hash aggregation spilling: applies 2x penalty for I/O operations due to typical hardware/OS behavior
- Spill cost calculation considers recursive partitioning depth and includes CPU costs for tuple spilling/reading
- Requires appropriately-sorted input when aggstrategy is AGG_SORTED
- Adds disable_cost penalty when enable_hashagg is false for hash-based strategies
- Output tuple count is set to 1 for AGG_PLAIN, numGroups for other strategies (adjusted by HAVING selectivity)

## Simplified Source

```c
void cost_agg(Path *path, PlannerInfo *root,
              AggStrategy aggstrategy, const AggClauseCosts *aggcosts,
              int numGroupCols, double numGroups, List *quals,
              Cost input_startup_cost, Cost input_total_cost,
              double input_tuples, double input_width) {
    double output_tuples;
    Cost startup_cost, total_cost;
    AggClauseCosts dummy_aggcosts;

    // Use dummy costs if aggcosts is NULL (grouping-only hash agg)
    if (aggcosts == NULL) {
        MemSet(&dummy_aggcosts, 0, sizeof(AggClauseCosts));
        aggcosts = &dummy_aggcosts;
    }

    if (aggstrategy == AGG_PLAIN) {
        // Single-group aggregation: all costs paid upfront
        startup_cost = input_total_cost + aggcosts->transCost.startup +
                      aggcosts->transCost.per_tuple * input_tuples +
                      aggcosts->finalCost.startup + aggcosts->finalCost.per_tuple;
        total_cost = startup_cost + cpu_tuple_cost;
        output_tuples = 1;
    }
    else if (aggstrategy == AGG_SORTED || aggstrategy == AGG_MIXED) {
        // On-the-fly delivery for sorted aggregation
        startup_cost = input_startup_cost;
        total_cost = input_total_cost + aggcosts->transCost.startup +
                    aggcosts->transCost.per_tuple * input_tuples +
                    (cpu_operator_cost * numGroupCols) * input_tuples +
                    aggcosts->finalCost.startup + aggcosts->finalCost.per_tuple * numGroups +
                    cpu_tuple_cost * numGroups;

        // Add disable penalty if needed
        if (aggstrategy == AGG_MIXED && !enable_hashagg) {
            startup_cost += disable_cost;
            total_cost += disable_cost;
        }
        output_tuples = numGroups;
    }
    else {
        // AGG_HASHED: all input processed before output
        startup_cost = input_total_cost + aggcosts->transCost.startup +
                      aggcosts->transCost.per_tuple * input_tuples +
                      (cpu_operator_cost * numGroupCols) * input_tuples +
                      aggcosts->finalCost.startup;

        if (!enable_hashagg)
            startup_cost += disable_cost;

        total_cost = startup_cost + aggcosts->finalCost.per_tuple * numGroups +
                    cpu_tuple_cost * numGroups;
        output_tuples = numGroups;
    }

    // Add spilling costs for hash aggregation if needed
    if (aggstrategy == AGG_HASHED || aggstrategy == AGG_MIXED) {
        // Estimate memory usage and spilling
        double hashentrysize = hash_agg_entry_size(list_length(root->aggtransinfos),
                                                  input_width, aggcosts->transitionSpace);
        Size mem_limit;
        uint64 ngroups_limit;
        int num_partitions;

        hash_agg_set_limits(hashentrysize, numGroups, 0, &mem_limit,
                           &ngroups_limit, &num_partitions);

        double nbatches = Max((numGroups * hashentrysize) / mem_limit,
                             numGroups / ngroups_limit);
        nbatches = Max(ceil(nbatches), 1.0);

        if (nbatches > 1.0) {
            // Calculate spilling I/O costs
            int depth = ceil(log(nbatches) / log(Max(num_partitions, 2)));
            double pages = relation_byte_size(input_tuples, input_width) / BLCKSZ;
            double pages_written = pages_read = pages * depth * 2.0; // 2x penalty

            startup_cost += pages_written * random_page_cost;
            total_cost += pages_written * random_page_cost + pages_read * seq_page_cost;

            // CPU cost for spilling/reading tuples
            double spill_cost = depth * input_tuples * 2.0 * cpu_tuple_cost;
            startup_cost += spill_cost;
            total_cost += spill_cost;
        }
    }

    // Apply HAVING clause costs and selectivity
    if (quals) {
        QualCost qual_cost;
        cost_qual_eval(&qual_cost, quals, root);
        startup_cost += qual_cost.startup;
        total_cost += qual_cost.startup + output_tuples * qual_cost.per_tuple;

        output_tuples = clamp_row_est(output_tuples *
                                     clauselist_selectivity(root, quals, 0, JOIN_INNER, NULL));
    }

    path->rows = output_tuples;
    path->startup_cost = startup_cost;
    path->total_cost = total_cost;
}
```