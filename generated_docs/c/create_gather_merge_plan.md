# create_gather_merge_plan

## Location
src/backend/optimizer/plan/createplan.c: 1958 - 2018

## Overview
Creates a GatherMerge plan node that performs ordered parallel execution by collecting and merging sorted results from multiple worker processes.

## Definition


## Detailed Description
The  function creates a GatherMerge plan node that coordinates parallel execution while preserving the sort order of results. Unlike regular Gather nodes that simply collect results in any order, GatherMerge performs an ordered merge of sorted streams from worker processes.

Key implementation details:
- **Ordered merge**: Merges sorted results from multiple worker processes while maintaining the overall sort order
- **Sort validation**: Verifies that the subplan is already sufficiently sorted for the required pathkeys, as additional sorting cannot be safely added at this level due to potential parallel-unsafe expressions
- **Projection pushdown**: Like Gather, pushes projection work to worker processes using CP_EXACT_TLIST for parallelization
- **Sort metadata**: Uses  to compute sort column information including operators, collations, and null ordering

The function creates the necessary sort infrastructure by populating sortColIdx, sortOperators, collations, and nullsFirst arrays that specify how to perform the ordered merge.

## Parameters / Member Variables
- : PlannerInfo containing planner state and execution context
- : GatherMergePath specifying the parallel merge strategy, pathkeys for ordering, and number of workers

## Dependencies
- Functions called/Symbols referenced:
  - build_path_tlist
  - create_plan_recurse (with CP_EXACT_TLIST flag)
  - makeNode
  - copy_generic_path_info
  - assign_special_exec_param
  - prepare_sort_from_pathkeys
  - pathkeys_contained_in
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- Requires non-empty pathkeys (sort order) - if no ordering is needed, a regular Gather should be used instead
- The subplan must already be sorted according to the required pathkeys; the function cannot add additional sorting due to potential parallel safety issues
- Uses a rescan parameter for coordinating rescans across parallel workers
- Automatically enables parallel mode by setting 
- The merge operation is performed using the sort operators, collations, and null handling specifications computed from the pathkeys
- Essential for queries that need both parallelism and ordered results, such as ORDER BY clauses or merge joins