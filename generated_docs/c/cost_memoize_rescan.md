# cost_memoize_rescan

## Location
src/backend/optimizer/path/costsize.c: 2509 - 2649

## Overview
Calculates the estimated costs for rescanning a Memoize node by analyzing cache hit ratios, memory constraints, and the distinctness of parameter values to determine caching effectiveness.

## Definition
static void cost_memoize_rescan(PlannerInfo *root, MemoizePath *mpath, Cost *rescan_startup_cost, Cost *rescan_total_cost)

## Detailed Description
The cost_memoize_rescan function estimates the cost of rescanning a Memoize node in PostgreSQL's query planner. Memoize nodes cache the results of expensive operations (typically subplans in nested loops) to avoid repeated computation when called with the same parameter values.

The function performs a sophisticated analysis that includes:
- Calculating available cache memory and estimating memory per cache entry
- Determining the number of distinct parameter values expected
- Computing cache hit ratio based on the relationship between distinct values and cache capacity
- Accounting for cache eviction costs when cache capacity is exceeded
- Factoring in overhead costs for cache operations (lookups, storage, evictions)

The costing model considers the worst-case scenario where no parameter value is seen twice (zero hit ratio) versus optimal cases with high cache hit ratios. If the estimation process falls back to default values for distinct parameter counts, the function conservatively assumes every call will have unique parameters.

## Parameters / Member Variables
- : PlannerInfo structure containing planner context and statistics
- : MemoizePath containing subpath information, parameter expressions, and call estimates
- : Output parameter for the calculated startup cost of rescans
- : Output parameter for the calculated total cost of rescans

## Dependencies
- Functions called/Symbols referenced:
  - get_hash_memory_limit (determines available cache memory)
  - relation_byte_size (calculates memory for cached tuples)
  - ExecEstimateCacheEntryOverheadBytes (estimates per-entry memory overhead)
  - get_expr_width (calculates parameter expression widths)
  - estimate_num_groups (estimates distinct parameter value counts)
  - SELFLAG_USED_DEFAULT (flag indicating default estimation fallback)
  - PG_UINT32_MAX (maximum value for cache entry limit)
- Called from (representative examples):
  - cost_rescan (in costsize.c:4619)

## Notes and Other Information
- Sets mpath->est_entries to guide executor hash table sizing decisions
- Uses conservative estimation when distinctness calculation falls back to defaults
- Charges different rates for various operations: cpu_operator_cost for lookups, cpu_tuple_cost for evictions
- Eviction overhead includes both entry removal and per-tuple cleanup (charged at cpu_operator_cost/10)
- Cache storage overhead applies to all tuples regardless of hit ratio
- Hit ratio calculation considers both the number of distinct values and cache capacity constraints
- Function is static, indicating it's only used within the costsize.c compilation unit