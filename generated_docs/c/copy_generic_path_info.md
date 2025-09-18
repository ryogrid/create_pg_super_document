# copy_generic_path_info

## Location
src/backend/optimizer/plan/createplan.c: 5410 - 5424

## Overview
Copies cost, size, and parallel-related information from a Path node to the Plan node created from it, providing essential execution statistics for EXPLAIN and parallel execution.

## Definition
```c
static void copy_generic_path_info(Plan *dest, Path *src)
```

## Detailed Description
This utility function transfers critical execution information from Path nodes (used during planning) to Plan nodes (used during execution). The function copies cost estimates (startup and total costs), size estimates (rows and width), and parallel execution flags from the source Path to the destination Plan.

While the executor typically doesn't use the cost and size information during normal query execution, this data is essential for EXPLAIN commands to show users the optimizer's estimates. The parallel-related flags (parallel_aware and parallel_safe) are actively used by the executor to determine how the plan can be executed in a parallel query context.

The function accesses the path target's width through src->pathtarget->width, which represents the estimated width in bytes of the tuples produced by this path.

## Parameters / Member Variables
- `dest`: Target Plan node to receive the copied information
- `src`: Source Path node containing the information to copy

## Dependencies
- Functions called/Symbols referenced:
  - (No function calls - direct field access only)
- Called from (representative examples):
  - create_seqscan_plan
  - create_indexscan_plan
  - create_nestloop_plan
  - create_hashjoin_plan
  - create_agg_plan
  - (Used by virtually all plan creation functions)

## Notes and Other Information
- This is a widely-used utility function called by nearly every plan creation function in createplan.c
- The cost and size information is primarily used for EXPLAIN output rather than execution decisions
- Parallel flags are crucial for the executor's parallel query execution logic
- The function performs shallow copying of scalar values - no deep copying of complex structures is needed
- Part of the standard pattern for converting Path nodes to Plan nodes during query planning