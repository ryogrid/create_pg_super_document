# UniquePath

## Location
src/include/nodes/pathnodes.h: 2027 - 2034

## Overview
UniquePath represents elimination of distinct rows from the output of its subpath, using hash-based, sort-based, or no-op implementation depending on the input characteristics.

## Definition
```c
typedef enum UniquePathMethod
{
	UNIQUE_PATH_NOOP,			/* input is known unique already */
	UNIQUE_PATH_HASH,			/* use hashing */
	UNIQUE_PATH_SORT,			/* use sorting */
} UniquePathMethod;

typedef struct UniquePath
{
	Path		path;
	Path	   *subpath;
	UniquePathMethod umethod;
	List	   *in_operators;	/* equality operators of the IN clause */
	List	   *uniq_exprs;		/* expressions to be made unique */
} UniquePath;
```

## Detailed Description
UniquePath represents the elimination of duplicate rows from its subpath output, primarily used to implement semi-joins efficiently. The path can use three different methods: no-op (when input is already proven unique), hash-based deduplication, or sort-based deduplication.

This path type is typically created during semi-join planning when the planner needs to ensure that the right-hand side of the join produces unique results. The choice of method depends on the data characteristics, available indexes, memory constraints, and cost estimates.

The no-op case occurs when the planner can prove the input is already unique through existing unique indexes or subquery analysis. Hash-based uniqueness is faster for smaller datasets that fit in memory, while sort-based uniqueness is more memory-efficient for larger datasets.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information like costs and row estimates
- `subpath`: The underlying path whose output needs to be made unique
- `umethod`: The method to use for eliminating duplicates (NOOP, HASH, or SORT)
- `in_operators`: List of equality operator OIDs used for comparing rows (from semi-join info)
- `uniq_exprs`: List of expressions that define uniqueness criteria (typically from semi-join RHS)

## Dependencies
- Functions called/Symbols referenced:
  - UniquePathMethod
- Called from (representative examples):
  - final_cost_mergejoin
  - final_cost_hashjoin
  - create_plan_recurse
  - create_unique_plan
  - create_unique_path

## Notes and Other Information
- Primarily used for semi-join optimization to ensure RHS uniqueness
- The planner caches UniquePath results in rel->cheapest_unique_path to avoid recomputation
- Hash method may be disabled if estimated memory usage exceeds hash_mem_multiplier * work_mem
- Sort method preserves any existing ordering in the input when possible
- No-op method is preferred when input uniqueness can be proven via indexes or subquery analysis
- Cost estimation compares hash vs sort methods and chooses the cheaper option
- Memory context handling ensures paths survive GEQO planning cycles when needed