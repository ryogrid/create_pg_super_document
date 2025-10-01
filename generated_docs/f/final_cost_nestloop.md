# final_cost_nestloop

## Location
[src/backend/optimizer/path/costsize.c:3308-3513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L3308-L3513)

## Overview
Provides the final estimate of the cost and result size of a nestloop join path, performing detailed cost calculations including CPU costs and join qualification evaluation.

## Definition

```c
void
final_cost_nestloop(PlannerInfo *root, NestPath *path,
					JoinCostWorkspace *workspace,
					JoinPathExtraData *extra)
```
## Detailed Description
This function performs the second phase of nested loop join cost estimation in PostgreSQL's query planner, building upon the preliminary estimates from initial_cost_nestloop. It provides comprehensive cost analysis including:

1. **Row count finalization**: Sets the final row estimate, accounting for parameterized paths and parallel execution scaling.

2. **Disable cost handling**: Adds disable_cost if nested loop joins are disabled via enable_nestloop.

3. **Special join type optimization**: For SEMI/ANTI joins or unique inner relations, calculates optimized costs based on early termination behavior:
   - Estimates scan fraction based on match distribution
   - Differentiates between indexed and non-indexed join scenarios
   - Accounts for unmatched outer rows requiring full inner scans

4. **CPU cost calculation**: Evaluates join restriction qualifications and adds per-tuple CPU costs including tuple processing overhead.

5. **Target list evaluation**: Adds costs for evaluating the output target list per result row.

The function handles complex scenarios like indexed join qualifications where unmatched rows may result in very cheap index probes returning no results.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and statistics
- : NestPath structure to be finalized with cost and row estimates
- : JoinCostWorkspace containing preliminary estimates from initial_cost_nestloop
- : JoinPathExtraData containing miscellaneous join information including semifactors

## Dependencies
- Functions called/Symbols referenced:
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [has_indexed_join_quals](../h/has_indexed_join_quals.md)
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - [NestPath](../N/NestPath.md)
  - [JoinCostWorkspace](../J/JoinCostWorkspace.md)
  - [JoinPathExtraData](../J/JoinPathExtraData.md)
  - [QualCost](../Q/QualCost.md)
  - Cost
  - JOIN_SEMI
  - JOIN_ANTI
- Called from (representative examples):
  - [create_nestloop_path](../c/create_nestloop_path.md)

## Notes and Other Information
- This is the second phase of the two-phase nested loop costing process
- Handles complex optimization for SEMI/ANTI joins with early scan termination
- Uses a fuzz factor of 2.0 when estimating scan fractions for matched rows
- Distinguishes between indexed and non-indexed join scenarios for cost accuracy
- Protects against zero row count assumptions that could cause division errors
- Accounts for parallel execution by scaling row estimates appropriately
- Target list evaluation costs are applied per output row, not per processed tuple

## Simplified Source

```c
void final_cost_nestloop(PlannerInfo *root, NestPath *path,
                        JoinCostWorkspace *workspace,
                        JoinPathExtraData *extra) {
    Path *outer_path = path->jpath.outerjoinpath;
    Path *inner_path = path->jpath.innerjoinpath;
    double outer_rows = outer_path->rows;
    double inner_rows = inner_path->rows;
    Cost startup_cost = workspace->startup_cost;
    Cost run_cost = workspace->run_cost;
    Cost cpu_per_tuple;
    QualCost restrict_qual_cost;
    double ntuples;

    // Protect against zero row estimates
    if (outer_rows <= 0) outer_rows = 1;
    if (inner_rows <= 0) inner_rows = 1;

    // Set final row estimate
    if (path->jpath.path.param_info)
        path->jpath.path.rows = path->jpath.path.param_info->ppi_rows;
    else
        path->jpath.path.rows = path->jpath.path.parent->rows;

    // Scale for parallel execution
    if (path->jpath.path.parallel_workers > 0) {
        double parallel_divisor = get_parallel_divisor(&path->jpath.path);
        path->jpath.path.rows = clamp_row_est(path->jpath.path.rows / parallel_divisor);
    }

    // Add disable cost if necessary
    if (!enable_nestloop)
        startup_cost += disable_cost;

    // Handle special join types (SEMI/ANTI/unique inner)
    if (path->jpath.jointype == JOIN_SEMI || path->jpath.jointype == JOIN_ANTI ||
        extra->inner_unique) {

        // Calculate early termination benefits
        double outer_matched_rows = rint(outer_rows * extra->semifactors.outer_match_frac);
        double outer_unmatched_rows = outer_rows - outer_matched_rows;
        double inner_scan_frac = 2.0 / (extra->semifactors.match_count + 1.0);

        ntuples = outer_matched_rows * inner_rows * inner_scan_frac;

        if (has_indexed_join_quals(path)) {
            // Indexed joins: cheap unmatched row handling
            run_cost += workspace->inner_run_cost * inner_scan_frac;
            if (outer_matched_rows > 1)
                run_cost += (outer_matched_rows - 1) * workspace->inner_rescan_run_cost * inner_scan_frac;
            run_cost += outer_unmatched_rows * workspace->inner_rescan_run_cost / inner_rows;
        } else {
            // Non-indexed: full scan costs
            ntuples += outer_unmatched_rows * inner_rows;
            run_cost += workspace->inner_run_cost;

            if (outer_unmatched_rows >= 1)
                outer_unmatched_rows -= 1;
            else
                outer_matched_rows -= 1;

            if (outer_matched_rows > 0)
                run_cost += outer_matched_rows * workspace->inner_rescan_run_cost * inner_scan_frac;
            if (outer_unmatched_rows > 0)
                run_cost += outer_unmatched_rows * workspace->inner_rescan_run_cost;
        }
    } else {
        // Regular join: process all tuple combinations
        ntuples = outer_rows * inner_rows;
    }

    // Add CPU costs for join qualification and tuple processing
    cost_qual_eval(&restrict_qual_cost, path->jpath.joinrestrictinfo, root);
    startup_cost += restrict_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + restrict_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    // Add target list evaluation costs (per output row)
    startup_cost += path->jpath.path.pathtarget->cost.startup;
    run_cost += path->jpath.path.pathtarget->cost.per_tuple * path->jpath.path.rows;

    // Set final costs
    path->jpath.path.startup_cost = startup_cost;
    path->jpath.path.total_cost = startup_cost + run_cost;
}
```