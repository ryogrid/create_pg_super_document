# create_append_path

## Location
src/backend/optimizer/util/pathnode.c: 1244 - 1374

## Overview
Creates a path node corresponding to an Append plan, which combines results from multiple child paths either sequentially or in parallel.

## Definition


## Detailed Description
This function constructs an AppendPath node that represents an Append operation in PostgreSQL's query execution plan. The Append operation combines results from multiple input paths, which can be either regular subpaths or partial subpaths for parallel execution. The function handles various optimization scenarios including single-child optimization (where the Append becomes a no-op), parallel execution with cost-based sorting, and proper parameter propagation.

For parallel-aware append operations, the function sorts non-partial paths by descending total costs and partial paths by descending startup costs to minimize total execution time. When there's only one child path with matching parallel awareness, the function optimizes by inheriting the child's costs and pathkeys directly.

## Parameters / Member Variables
- : PlannerInfo context (can be NULL for some callers)
- : RelOptInfo for the relation this path represents
- : List of regular child paths to append
- : List of partial paths for parallel execution
- : Sort ordering required for the output (must be NIL for parallel-aware paths)
- : Set of outer relids required by this path
- : Number of parallel workers (must be > 0 if parallel_aware is true)
- : Whether this is a parallel-aware append operation
- : Optional row count override (use -1 to calculate from subpaths)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (AppendPath creation)
  - get_baserel_parampathinfo
  - get_appendrel_parampathinfo
  - list_sort
  - append_total_cost_compare
  - append_startup_cost_compare
  - list_concat
  - bms_equal
  - cost_append
  - PATH_REQ_OUTER
- Called from (representative examples):
  - add_paths_to_append_rel
  - generate_orderedappend_paths
  - generate_union_paths
  - set_dummy_rel_pathlist

## Notes and Other Information
- Handles the special case of NIL subpaths representing dummy access paths
- For baserels with root context, uses more comprehensive ParamPathInfo construction to support Memoize paths and runtime pruning
- Applies query-wide LIMIT when the path represents the sole base relation
- Single-child Append paths are optimized to inherit child properties when parallel awareness matches
- All child paths must have the same parameterization (required_outer)
- For parallel-aware appends, pathkeys must be NIL to allow cost-based sorting of subpaths