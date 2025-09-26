# Memoize

## Location
[src/include/nodes/plannodes.h:889-925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L889-L925)

## Overview
Memoize is a caching plan node that stores results from parameterized child nodes to avoid re-scanning when the same parameter values are encountered again.

## Definition

```c
typedef struct Memoize
{
	Plan		plan;

	/* size of the two arrays below */
	int			numKeys;

	/* hash operators for each key */
	Oid		   *hashOperators pg_node_attr(array_size(numKeys));

	/* collations for each key */
	Oid		   *collations pg_node_attr(array_size(numKeys));

	/* cache keys in the form of exprs containing parameters */
	List	   *param_exprs;

	/*
	 * true if the cache entry should be marked as complete after we store the
	 * first tuple in it.
	 */
	bool		singlerow;

	/*
	 * true when cache key should be compared bit by bit, false when using
	 * hash equality ops
	 */
	bool		binary_mode;

	/*
	 * The maximum number of entries that the planner expects will fit in the
	 * cache, or 0 if unknown
	 */
	uint32		est_entries;

	/* paramids from param_exprs */
	Bitmapset  *keyparamids;
} Memoize;
```
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
  - [Plan](../P/Plan.md) (inherited base structure)
  - [List](../L/List.md) (for param_exprs)
  - Oid (for operators and collations)
  - [Bitmapset](../B/Bitmapset.md) (for keyparamids)

- Called from (representative examples):
  - [show_memoize_info](../s/show_memoize_info.md) (commands/explain.c:3350)
  - [ExecInitNode](../E/ExecInitNode.md) (executor/execProcnode.c:331)
  - [ExecMemoize](../E/ExecMemoize.md) (executor/nodeMemoize.c:724)
  - [ExecInitMemoize](../E/ExecInitMemoize.md) (executor/nodeMemoize.c:952)
  - [create_memoize_plan](../c/create_memoize_plan.md) (optimizer/plan/createplan.c:1669)
  - [make_memoize](../m/make_memoize.md) (optimizer/plan/createplan.c:6573)

## Notes and Other Information
- Introduced in PostgreSQL 14 as a performance optimization for parameterized queries
- Uses hash table with LRU eviction policy, never spilling to disk unlike some other caching mechanisms
- Particularly beneficial for nested loop joins and correlated subqueries with repeated parameter values
- The cache is bounded by work_mem and uses a bypass mode when memory is exhausted
- Performance benefits are most significant when parameter value cardinality is much lower than the number of tuples processed
- Cache effectiveness is reported in EXPLAIN ANALYZE output showing hit rates and memory usage
- The pg_node_attr annotations ensure proper handling of variable-length arrays during node copying and serialization operations