# postprocess_setop_rel

## Location
[src/backend/optimizer/prep/prepunion.c:1272-1289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1272-L1289)

## Overview
Performs final processing steps for a set operation RelOptInfo after all paths have been added, including extension hook invocation and cheapest path selection.

## Definition


## Detailed Description
This function serves as a finalization step in the set operation planning process. It handles two main responsibilities:

1. **Extension Hook Invocation**: Calls the  if registered, allowing extensions and FDWs to contribute additional paths to the set operation relation. The hook is called with UPPERREL_SETOP to indicate this is a set operation upper relation.

2. **Path Selection**: Calls  to analyze all available paths in the relation and select the one with the lowest estimated cost as the cheapest path. This sets both the cheapest_startup_path and cheapest_total_path fields in the RelOptInfo.

The function is intentionally lightweight as the heavy lifting of path generation is handled by the specific set operation functions (, , etc.). Its primary purpose is to ensure proper finalization and allow for extensibility.

## Parameters / Member Variables
- : PlannerInfo containing the global planning context and configuration settings
- : RelOptInfo for the set operation that needs post-processing after all paths have been added

## Dependencies
- Functions called/Symbols referenced:
  - create_upper_paths_hook (global function pointer)
  - [set_cheapest](../s/set_cheapest.md)
  - UPPERREL_SETOP (constant)
- Called from (representative examples):
  - [recurse_set_operations](../r/recurse_set_operations.md)
  - [generate_recursion_path](../g/generate_recursion_path.md)  
  - [build_setop_child_paths](../b/build_setop_child_paths.md)

## Notes and Other Information
- The function currently does not actively support FDW path contributions for set operations, but the hook mechanism allows for future extensibility
- The hook is called with NULL for both the extra and parent_rel parameters since set operations don't currently use these
- This function must be called after all paths have been generated and added to the relation to ensure proper cheapest path selection
- The function is a critical step in the planning pipeline as many other parts of the planner depend on having valid cheapest paths set