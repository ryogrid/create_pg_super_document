# Memoize

## Location
src/include/nodes/plannodes.h: 889 - 925

## Overview
Memoize is a caching plan node that stores results from parameterized child nodes to avoid re-scanning when the same parameter values are encountered again.

## Definition


## Detailed Description
The Memoize node is a sophisticated caching mechanism introduced in PostgreSQL 14 to optimize parameterized queries by storing results from expensive child operations. It sits above parameterized nodes in the plan tree and maintains an in-memory hash table cache to avoid redundant computation when the same parameter values are seen repeatedly.

The node is particularly effective in scenarios like nested loop joins where the inner side is repeatedly executed with different parameter values from the outer side. Instead of re-executing the expensive inner plan for each parameter combination, Memoize can return cached results when the same parameters are encountered again.

The cache uses an LRU (Least Recently Used) eviction policy when memory limits are reached. When the cache becomes full, the least recently used entries are evicted to make room for new ones. The implementation never spills to disk - instead it uses a bypass mode when memory is exhausted, falling back to direct execution of the child plan.

The singlerow flag optimizes for unique joins where only one tuple is expected per parameter set, allowing the cache entry to be marked complete immediately. Binary mode enables bit-by-bit comparison of cache keys for improved performance when appropriate.

## Parameters / Member Variables
- : Base Plan structure containing target list, qualifications, cost estimates, and child plan references
- : Number of cache key expressions and corresponding size of the hashOperators and collations arrays
- : Array of hash operator OIDs used for hashing and comparing cache key values
- : Array of collation OIDs for proper comparison of cache keys with locale-specific ordering
- : List of expressions containing parameters that form the cache keys for lookup and storage
- : Boolean flag indicating whether cache entries should be marked complete after storing just the first tuple (optimization for unique joins)
- : Boolean flag controlling whether cache keys should be compared using bitwise comparison (true) or hash equality operations (false)
- : Planner's estimate of the maximum number of cache entries that will fit in memory, used for cache sizing decisions
- : Bitmap set containing the parameter IDs referenced in the cache key expressions

## Dependencies
- Functions called/Symbols referenced:
  - Plan (inherited base structure)
  - List (for param_exprs)
  - Oid (for operators and collations)
  - Bitmapset (for keyparamids)

- Called from (representative examples):
  - show_memoize_info (commands/explain.c:3350)
  - ExecInitNode (executor/execProcnode.c:331)
  - ExecMemoize (executor/nodeMemoize.c:724)
  - ExecInitMemoize (executor/nodeMemoize.c:952)
  - create_memoize_plan (optimizer/plan/createplan.c:1669)
  - make_memoize (optimizer/plan/createplan.c:6573)

## Notes and Other Information
- Introduced in PostgreSQL 14 as a performance optimization for parameterized queries
- Uses hash table with LRU eviction policy, never spilling to disk unlike some other caching mechanisms
- Particularly beneficial for nested loop joins and correlated subqueries with repeated parameter values
- The cache is bounded by work_mem and uses a bypass mode when memory is exhausted
- Performance benefits are most significant when parameter value cardinality is much lower than the number of tuples processed
- Cache effectiveness is reported in EXPLAIN ANALYZE output showing hit rates and memory usage
- The pg_node_attr annotations ensure proper handling of variable-length arrays during node copying and serialization operations