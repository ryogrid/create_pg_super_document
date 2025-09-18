# create_append_plan

## Location
src/backend/optimizer/plan/createplan.c: 1217 - 1437

## Overview
Creates an Append plan node that combines results from multiple subpaths, with support for sorting, asynchronous execution, and partition pruning.

## Definition
```c
static Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path, int flags)
```

## Detailed Description
The `create_append_plan` function builds an Append execution plan that concatenates results from multiple child plans. This is commonly used for operations like UNION ALL, partitioned table access, or inheritance hierarchies. The function handles several complex scenarios: it can generate a dummy Result plan when no subpaths exist, create sorted Append plans by inserting Sort nodes where needed, enable asynchronous execution for eligible subplans, and set up partition pruning information for runtime optimization. The function ensures all child plans produce compatible target lists and manages the complexity of coordinating multiple execution paths.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information
- `best_path`: AppendPath structure representing the selected append strategy with its subpaths
- `flags`: Control flags affecting plan creation (CP_EXACT_TLIST, CP_SMALL_TLIST, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - build_path_tlist
  - make_result
  - makeBoolConst
  - copy_generic_path_info
  - prepare_sort_from_pathkeys
  - create_plan_recurse
  - pathkeys_contained_in
  - make_sort
  - label_sort_with_costsize
  - mark_async_capable_plan
  - extract_actual_clauses
  - replace_nestloop_params
  - make_partition_pruneinfo
  - inject_projection_plan
  - Append (type)
  - AppendPath (type)
  - PartitionPruneInfo (type)
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- Generates a dummy Result plan with constant-FALSE gating when no subpaths exist (empty relation case)
- For ordered Appends, ensures all children produce the same sort key columns and inserts Sort nodes as needed
- Supports asynchronous execution when enable_async_append is true, pathkeys are NIL, and the path is not parallel_safe
- Implements partition pruning by gathering baserestrictinfo and parameter clauses for runtime pruning
- Handles target list compatibility by ensuring all children return the same tlist structure
- May inject a projection plan to remove sort columns added during planning if exact or small tlist is required
- The nasyncplans counter tracks how many subplans can execute asynchronously
- Uses extensive assertion checking to validate sort key consistency across subplans