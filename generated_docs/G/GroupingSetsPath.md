# GroupingSetsPath

## Location
[src/include/nodes/pathnodes.h:2295-2303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2295-L2303)

## Overview
GroupingSetsPath represents a GROUPING SETS aggregation path node in PostgreSQL's query planner, used to handle complex aggregation operations involving multiple grouping sets.

## Definition

```c
typedef struct GroupingSetsPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
	AggStrategy aggstrategy;	/* basic strategy */
	List	   *rollups;		/* list of RollupData */
	List	   *qual;			/* quals (HAVING quals), if any */
	uint64		transitionSpace;	/* for pass-by-ref transition data */
} GroupingSetsPath;
```
## Detailed Description
GroupingSetsPath is a specialized path node that handles GROUPING SETS aggregation operations in PostgreSQL's query planning phase. It extends the base Path structure to represent execution plans for queries that use GROUPING SETS, ROLLUP, or CUBE clauses. This path type encapsulates the strategy for performing multi-level aggregations efficiently, including the underlying input path and the aggregation strategy to be employed.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information like cost estimates and output properties
- `*subpath`: Pointer to the input path that provides the source data for the grouping sets operation
- `aggstrategy`: The aggregation strategy to use (e.g., AGG_PLAIN, AGG_SORTED, AGG_HASHED)
- `*rollups`: List of RollupData structures that define the specific grouping sets and their relationships
- `*qual`: List of qualification expressions (HAVING clauses) to be applied after aggregation
- `transitionSpace`: Estimated memory space required for pass-by-reference transition data during aggregation
## Dependencies
- Functions called/Symbols referenced:
  - AggStrategy
- Called from (representative examples):
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md)
  - [create_groupingsets_path](../c/create_groupingsets_path.md)
  - [create_plan_recurse](../c/create_plan_recurse.md)

## Notes and Other Information
- This path type is specifically designed for handling complex GROUP BY operations with multiple grouping levels
- The rollups list contains RollupData structures that define the hierarchical grouping relationships
- Memory estimation via transitionSpace is crucial for choosing between different aggregation strategies
- The qual field allows HAVING clauses to be efficiently applied after the grouping operation