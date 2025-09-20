# GroupingSetKind

## Location
[src/include/nodes/parsenodes.h:1504-1505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1504-L1505)

## Overview
GroupingSetKind is an enumeration that defines the different types of grouping sets used in PostgreSQL's GROUP BY clause, including CUBE, ROLLUP, and GROUPING SETS operations for advanced aggregation functionality.

## Definition

```c
typedef struct GroupingSet
{
	NodeTag		type;
	GroupingSetKind kind pg_node_attr(query_jumble_ignore);
	List	   *content;
	ParseLoc	location;
} GroupingSet;
```
## Detailed Description
GroupingSetKind classifies the various types of grouping sets that can appear in SQL GROUP BY clauses with advanced aggregation features. This enumeration supports the SQL standard's grouping sets functionality, allowing for complex aggregation patterns like hierarchical rollups, multidimensional cubes, and arbitrary grouping combinations. The different kinds represent various stages of parse analysis transformation from raw syntax to the final optimized representation.

## Parameters / Member Variables
- `GROUPING_SET_EMPTY`: Represents empty grouping sets (), typically used for grand totals
- `GROUPING_SET_SIMPLE`: Represents a list of expressions treated as an atomic grouping unit
- `GROUPING_SET_ROLLUP`: Represents ROLLUP operations for hierarchical aggregations
- `GROUPING_SET_CUBE`: Represents CUBE operations for multidimensional aggregations
- `GROUPING_SET_SETS`: Represents explicit GROUPING SETS clauses containing other grouping set types

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - [makeGroupingSet](../m/makeGroupingSet.md) (src/backend/nodes/makefuncs.c:864)
  - MAKEFUNC_H (src/include/nodes/makefuncs.h:108)
  - GroupingSet (src/include/nodes/parsenodes.h:1509)

## Notes and Other Information
- [GroupingSetKind](GroupingSetKind.md) is essential for implementing SQL:1999 GROUP BY extensions
- EMPTY nodes are used when no grouping columns are specified but aggregation is still needed
- SIMPLE nodes contain integer lists of ressortgroupref values after parse analysis
- CUBE and ROLLUP nodes contain lists of SIMPLE nodes in their final form
- SETS nodes provide the top-level container for complex grouping set combinations
- The transformation from raw parsing to final form eliminates arbitrary nesting while preserving semantic meaning
- Example:  becomes 