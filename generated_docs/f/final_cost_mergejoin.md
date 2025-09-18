# final_cost_mergejoin

## Location
src/backend/optimizer/path/costsize.c: 3745 - 3993

## Overview
Computes the final cost estimate and result size of a mergejoin path, making critical decisions about mark/restore optimization and inner relation materialization based on cost analysis.

## Definition


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
- : PlannerInfo containing query planning context and statistics
- : MergePath being costed (updated with final cost and execution decisions)
- : JoinCostWorkspace from initial_cost_mergejoin containing preliminary estimates
- : JoinPathExtraData with miscellaneous join information including inner_unique flag

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