# create_memoize_path

## Location
src/backend/optimizer/util/pathnode.c: 1598 - 1653

## Overview
Creates a MemoizePath node that represents a Memoize plan operation, which caches the results of its subpath based on parameter values to avoid redundant computations in nested loop scenarios.

## Definition
```c
MemoizePath *create_memoize_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                                List *param_exprs, List *hash_operators,
                                bool singlerow, bool binary_mode, double calls)
```

## Detailed Description
The `create_memoize_path` function constructs a MemoizePath node that corresponds to a Memoize plan node in PostgreSQL's query execution. A Memoize node acts as a cache layer that stores results from its subpath based on the values of specified parameters. This optimization is particularly effective in nested loop joins where the inner relation is repeatedly scanned with different parameter values, allowing previously computed results to be reused when the same parameter combination is encountered again.

The function initializes the MemoizePath structure with caching-specific properties including parameter expressions used as cache keys, hash operators for efficient key comparison, and execution mode flags. The cost calculation adds a small overhead for caching the first entry, while more complex rescan costs are handled by the cost_memoize_rescan function during planning.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `rel`: The RelOptInfo representing the relation that this memoize path will produce
- `subpath`: The input Path whose results will be cached
- `param_exprs`: List of expressions that serve as cache keys (parameters to memoize on)
- `hash_operators`: List of hash operators for the parameter expressions
- `singlerow`: Boolean indicating whether the subpath is expected to return at most one row
- `binary_mode`: Boolean indicating whether to use binary comparison for cache keys
- `calls`: Estimated number of calls (clamped to a reasonable range)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the MemoizePath node)
  - clamp_row_est (to ensure the calls estimate is within reasonable bounds)
  - MemoizePath (the path node type being created)
- Called from (representative examples):
  - get_memoize_path (when considering memoization during join planning)
  - reparameterize_path (when reparameterizing paths)

## Notes and Other Information
- The est_entries field is initially set to 0, with the actual estimation left to cost_memoize_rescan during costing
- The function adds a small cpu_tuple_cost overhead to both startup and total costs for caching the first entry
- More complex cost calculations for rescans are handled separately by cost_memoize_rescan
- The memoize path preserves the pathkeys and parallelization properties of its subpath
- Binary mode affects how cache key comparisons are performed for efficiency
- The singlerow flag can enable optimizations when the subpath is known to return at most one tuple