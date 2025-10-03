# final_cost_mergejoin

## Location
[src/backend/optimizer/path/costsize.c:3745-3993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L3745-L3993)

## Overview
Computes the final cost estimate and result size of a mergejoin path, making critical decisions about mark/restore optimization and inner relation materialization based on cost analysis.

## Definition

```c
void
final_cost_mergejoin(PlannerInfo *root, MergePath *path,
					 JoinCostWorkspace *workspace,
					 JoinPathExtraData *extra)
```
## Detailed Description
This function finalizes the cost estimation for a merge join operation, making two important execution decisions that affect performance:

1. **Mark/Restore Decision**: Determines whether the executor needs to perform mark/restore operations during the merge join. This can be skipped for SEMI/ANTI joins or when the inner relation is unique and all join clauses are merge clauses.

2. **Materialization Decision**: Decides whether to materialize the inner path to optimize mark/restore operations. Materialization is chosen when it's cheaper than repeated rescanning, when the inner path doesn't support mark/restore, or when sorting is expected to spill to disk.

The function calculates the total cost by considering:
- CPU costs for tuple comparisons and qualification evaluation
- Rescanning overhead when there are equal merge keys in the outer relation
- Materialization costs vs. bare inner path costs
- Parallel execution adjustments

Unlike other cost functions, this routine makes actual execution decisions rather than just estimating costs, because the choice between alternatives doesn't affect pathkeys or startup cost.

## Parameters / Member Variables
- `*root`: PlannerInfo containing query planning context and statistics
- `*path`: MergePath being costed (updated with final cost and execution decisions)
- `*workspace`: JoinCostWorkspace from initial_cost_mergejoin containing preliminary estimates
- `*extra`: JoinPathExtraData with miscellaneous join information including inner_unique flag
## Dependencies
- Functions called/Symbols referenced:
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - [approx_tuple_count](../a/approx_tuple_count.md)
  - [ExecSupportsMarkRestore](../E/ExecSupportsMarkRestore.md)
  - [relation_byte_size](../r/relation_byte_size.md)
- Called from (representative examples):
  - [create_mergejoin_path](../c/create_mergejoin_path.md)

## Notes and Other Information
- Sets path->skip_mark_restore and path->materialize_inner flags based on cost analysis
- Uses rescan ratio to estimate tuple re-fetching overhead from duplicate merge keys
- Assumes materialized nodes won't spill to disk since they only need to remember tuples back to the last mark
- Adds disable_cost penalty if enable_mergejoin is disabled
- For parallel paths, scales row estimates using parallel divisor
- The cost model assumes re-fetch cost equals original fetch cost, which may be conservative

## Simplified Source

```c
void final_cost_mergejoin(PlannerInfo *root, MergePath *path,
                         JoinCostWorkspace *workspace,
                         JoinPathExtraData *extra) {
    Path *outer_path = path->jpath.outerjoinpath;
    Path *inner_path = path->jpath.innerjoinpath;
    double inner_path_rows = inner_path->rows;
    List *mergeclauses = path->path_mergeclauses;
    List *innersortkeys = path->innersortkeys;
    Cost startup_cost = workspace->startup_cost;
    Cost run_cost = workspace->run_cost;
    Cost inner_run_cost = workspace->inner_run_cost;
    double outer_rows = workspace->outer_rows;
    double inner_rows = workspace->inner_rows;
    double outer_skip_rows = workspace->outer_skip_rows;
    double inner_skip_rows = workspace->inner_skip_rows;

    // Protect against zero row estimates
    if (inner_path_rows <= 0) inner_path_rows = 1;

    // Set final row estimate and handle parallel execution
    if (path->jpath.path.param_info)
        path->jpath.path.rows = path->jpath.path.param_info->ppi_rows;
    else
        path->jpath.path.rows = path->jpath.path.parent->rows;

    if (path->jpath.path.parallel_workers > 0) {
        double parallel_divisor = get_parallel_divisor(&path->jpath.path);
        path->jpath.path.rows = clamp_row_est(path->jpath.path.rows / parallel_divisor);
    }

    // Add disable cost if necessary
    if (!enable_mergejoin)
        startup_cost += disable_cost;

    // Calculate qualification costs
    QualCost merge_qual_cost, qp_qual_cost;
    cost_qual_eval(&merge_qual_cost, mergeclauses, root);
    cost_qual_eval(&qp_qual_cost, path->jpath.joinrestrictinfo, root);
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    // Decide if we can skip mark/restore (SEMI/ANTI joins or unique inner)
    if ((path->jpath.jointype == JOIN_SEMI || path->jpath.jointype == JOIN_ANTI || extra->inner_unique) &&
        (list_length(path->jpath.joinrestrictinfo) == list_length(path->path_mergeclauses)))
        path->skip_mark_restore = true;
    else
        path->skip_mark_restore = false;

    // Estimate tuples and rescanning overhead
    double mergejointuples = approx_tuple_count(root, &path->jpath, mergeclauses);
    double rescannedtuples;

    if (IsA(outer_path, UniquePath) || path->skip_mark_restore)
        rescannedtuples = 0;
    else {
        rescannedtuples = mergejointuples - inner_path_rows;
        if (rescannedtuples < 0) rescannedtuples = 0;
    }

    double rescanratio = 1.0 + (rescannedtuples / inner_rows);

    // Calculate materialization vs bare inner costs
    Cost bare_inner_cost = inner_run_cost * rescanratio;
    Cost mat_inner_cost = inner_run_cost + cpu_operator_cost * inner_rows * rescanratio;

    // Decide whether to materialize inner relation
    if (path->skip_mark_restore) {
        path->materialize_inner = false;
    } else if (enable_material && mat_inner_cost < bare_inner_cost) {
        path->materialize_inner = true;
    } else if (innersortkeys == NIL && !ExecSupportsMarkRestore(inner_path)) {
        // Force materialization if inner doesn't support mark/restore
        path->materialize_inner = true;
    } else if (enable_material && innersortkeys != NIL &&
               relation_byte_size(inner_path_rows, inner_path->pathtarget->width) > (work_mem * 1024L)) {
        // Materialize if sort will spill to disk
        path->materialize_inner = true;
    } else {
        path->materialize_inner = false;
    }

    // Add chosen inner path cost
    if (path->materialize_inner)
        run_cost += mat_inner_cost;
    else
        run_cost += bare_inner_cost;

    // Add CPU costs for merge comparisons and qualifications
    startup_cost += merge_qual_cost.startup;
    startup_cost += merge_qual_cost.per_tuple * (outer_skip_rows + inner_skip_rows * rescanratio);
    run_cost += merge_qual_cost.per_tuple * ((outer_rows - outer_skip_rows) +
                                            (inner_rows - inner_skip_rows) * rescanratio);

    // Add costs for additional restrictions and tuple processing
    startup_cost += qp_qual_cost.startup;
    Cost cpu_per_tuple = cpu_tuple_cost + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * mergejointuples;

    // Add target list evaluation costs
    startup_cost += path->jpath.path.pathtarget->cost.startup;
    run_cost += path->jpath.path.pathtarget->cost.per_tuple * path->jpath.path.rows;

    // Set final costs
    path->jpath.path.startup_cost = startup_cost;
    path->jpath.path.total_cost = startup_cost + run_cost;
}
```