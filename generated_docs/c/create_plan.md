# create_plan

## Location
src/backend/optimizer/plan/createplan.c: 338 - 388

## Overview
Creates the access plan for a query by recursively processing the desired tree of path nodes, starting at the best path and generating corresponding plan nodes.

## Definition
```c
Plan *create_plan(PlannerInfo *root, Path *best_path)
```

## Detailed Description
The create_plan function serves as the main entry point for converting an optimized path tree into an executable plan tree. It takes the best access path determined by the optimizer and recursively transforms it into a tree of plan nodes. The function initializes the planning workspace, calls create_plan_recurse to perform the recursive conversion, and then applies final adjustments to ensure the top-level plan node has proper column naming and initialization plans are properly attached.

The target lists and qualifications in the generated plan tree remain in planner format with Vars corresponding to the parser's numbering, which will be fixed later by setrefs.c. The function ensures proper handling of NestLoopParams and maintains plan parameter consistency throughout the planning process.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information for the current query level
- `best_path`: The optimal access path determined by the path-based optimizer, representing the execution strategy to be converted into a plan

## Dependencies
- Functions called/Symbols referenced:
  - create_plan_recurse
  - apply_tlist_labeling
  - SS_attach_initplans
  - ModifyTable (type check)
  - CP_EXACT_TLIST (constant)
- Called from (representative examples):
  - standard_planner
  - make_subplan
  - create_subqueryscan_plan
  - create_minmaxagg_plan
  - SS_process_ctes

## Notes and Other Information
- Assumes plan_params is not in use at the current query level and resets it after processing
- Applies target list labeling only for non-ModifyTable plan nodes to preserve original column names
- Attaches any initialization plans created during query processing to the topmost plan node
- Validates that all NestLoopParams are properly assigned to plan nodes
- The function is located at src/backend/optimizer/plan/createplan.c:338-388