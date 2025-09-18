# create_memoize_plan

## Location
[src/backend/optimizer/plan/createplan.c:1667-1720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1667-L1720)

## Overview
Creates a Memoize plan node that caches results based on parameter values, providing an efficient caching mechanism for parameterized subplans that are likely to be called repeatedly with the same parameter values.

## Definition
```c
static Memoize *
create_memoize_plan(PlannerInfo *root, MemoizePath *best_path, int flags)
```

## Detailed Description
The `create_memoize_plan` function creates a Memoize execution plan node from a MemoizePath. The Memoize node implements a sophisticated caching strategy that stores results from its child plan indexed by parameter values. This is particularly beneficial for nested loop joins where the inner relation is parameterized and likely to be called multiple times with repeated parameter values.

The function performs several key setup operations:
1. Creates the child plan with a small target list to minimize cache memory usage
2. Processes parameter expressions and replaces nestloop parameters
3. Sets up hash operators and collations for the cache key
4. Configures the cache with estimated entry count and operational modes
5. Extracts parameter IDs that will be used as cache keys

The Memoize node maintains a hash table where keys are parameter value combinations and values are the corresponding result sets. This can dramatically improve performance for queries with repeated parameter patterns.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: MemoizePath containing parameterized path information, hash operators, and cache configuration
- `flags`: Control flags affecting target list handling, passed to child with CP_SMALL_TLIST added

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md) (creates the child plan to be memoized)
  - [replace_nestloop_params](../r/replace_nestloop_params.md) (processes parameter expressions for the current context)
  - [exprCollation](../e/exprCollation.md) (extracts collation information from parameter expressions)
  - [pull_paramids](../p/pull_paramids.md) (extracts parameter IDs from expressions)
  - [make_memoize](../m/make_memoize.md) (creates the Memoize plan node with cache configuration)
  - [copy_generic_path_info](copy_generic_path_info.md) (copies common path information to the plan)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md) (main recursive plan creation function)

## Notes and Other Information
- Memoization is most effective when the same parameter values are likely to be seen multiple times
- The cache can operate in binary mode for more efficient storage when appropriate
- Cache size is estimated based on the MemoizePath's est_entries field
- Single-row mode optimizes for cases where each parameter combination produces at most one row
- Hash operators from the MemoizePath determine how parameter values are hashed for cache lookup
- Memory usage is bounded by work_mem settings and the estimated number of cache entries
- Particularly beneficial for nested loops with expensive inner relations or complex parameter-dependent computations