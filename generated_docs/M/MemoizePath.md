# MemoizePath

## Location
[src/include/nodes/pathnodes.h:1992-2006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1992-L2006)

## Overview
MemoizePath represents a Memoize plan node that caches tuples from parameterized paths to avoid rescanning for parameter values that are already cached.

## Definition
```c
typedef struct MemoizePath
{
	Path		path;
	Path	   *subpath;		/* outerpath to cache tuples from */
	List	   *hash_operators; /* OIDs of hash equality ops for cache keys */
	List	   *param_exprs;	/* expressions that are cache keys */
	bool		singlerow;		/* true if the cache entry is to be marked as
							 * complete after caching the first record. */
	bool		binary_mode;	/* true when cache key should be compared bit
							 * by bit, false when using hash equality ops */
	Cardinality calls;			/* expected number of rescans */
	uint32		est_entries;	/* The maximum number of entries that the
							 * planner expects will fit in the cache, or 0
							 * if unknown */
} MemoizePath;
```

## Detailed Description
MemoizePath represents a sophisticated caching mechanism for parameterized plans in PostgreSQL. Unlike MaterialPath which caches all output, MemoizePath creates a parameter-aware cache that stores results keyed by parameter values. When the same parameter values are encountered again, the cached results are returned instead of re-executing the subpath.

This is particularly effective for nested loop joins where the inner path is parameterized by values from the outer path. If many outer rows have the same parameter values, the Memoize node can dramatically reduce execution time by serving cached results for repeated parameter combinations.

The cache uses either hash-based equality comparison or bit-by-bit binary comparison for cache keys, depending on the binary_mode setting.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information
- `subpath`: The underlying parameterized path whose output will be cached
- `hash_operators`: List of OID values for hash equality operators used for cache key comparison
- `param_exprs`: List of expressions that serve as cache keys (typically parameter references)
- `singlerow`: When true, marks cache entries as complete after storing the first record (optimization for unique results)
- `binary_mode`: When true, uses bit-by-bit comparison for cache keys instead of hash equality operators
- `calls`: Expected number of times this path will be rescanned (used for cost estimation)
- `est_entries`: Estimated maximum cache entries that will fit in memory (0 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - Cardinality
- Called from (representative examples):
  - cost_memoize_rescan
  - cost_rescan
  - create_plan_recurse
  - create_memoize_plan
  - create_memoize_path
  - reparameterize_path

## Notes and Other Information
- Introduced as an optimization for parameterized nested loop joins
- Most effective when there are many duplicate parameter value combinations
- The singlerow optimization is used when the subpath is known to return at most one row per parameter combination
- Binary mode comparison is faster but requires cache keys to have the same binary representation for equal values
- Cache size is limited by work_mem setting in the executor
- Cost estimation involves complex calculations to predict cache hit rates and memory usage
- The executor makes runtime decisions about cache size if est_entries is 0