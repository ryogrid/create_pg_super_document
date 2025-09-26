# GroupingSet

## Location
src/include/nodes/parsenodes.h: 1506 - 1512

## Overview
GroupingSet represents the structure of CUBE, ROLLUP, and GROUPING SETS clauses in GROUP BY statements, providing a hierarchical representation that captures the syntactic organization of complex grouping operations.

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
GroupingSet nodes form a tree structure that represents the organization of complex GROUP BY clauses involving CUBE, ROLLUP, and GROUPING SETS operations. The parser initially creates a mixed structure reflecting the query syntax, which is then transformed during parse analysis into a standardized format.

The transformation process converts expressions into targetlist references and builds a fixed hierarchical structure:
- EMPTY nodes represent empty grouping sets ()
- SIMPLE nodes contain lists of ressortgroupref values referencing expressions in the targetlist
- CUBE and ROLLUP nodes contain lists of SIMPLE nodes
- SETS nodes contain lists of EMPTY, SIMPLE, CUBE, or ROLLUP nodes

This design enables PostgreSQL to efficiently process complex grouping operations while preserving the logical structure needed for correct query execution.

## Parameters / Member Variables
- : NodeTag identifying this as a GroupingSet node
- : GroupingSetKind enumeration specifying the type (EMPTY, SIMPLE, CUBE, ROLLUP, or SETS)
- : List containing the contents of this grouping set (expressions, ressortgroupref values, or nested GroupingSet nodes depending on processing stage and kind)
- : ParseLoc indicating the location in the source query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - GroupingSetKind (enumeration for node types)
  - ParseLoc (for source location tracking)
  - List (for content storage)
- Called from (representative examples):
  - transformGroupingSet (parser/parse_clause.c)
  - expand_groupingset_node (parser/parse_agg.c)
  - flatten_grouping_sets (parser/parse_clause.c)
  - makeGroupingSet (nodes/makefuncs.c)

## Notes and Other Information
- The structure evolves during parsing: raw parser output contains expressions and potentially nested structures, while analyzed output uses standardized ressortgroupref values
- A query can have an empty groupClause but still be an aggregation query if the groupingSets tree contains only EMPTY nodes
- The design supports the complex transformations required by the SQL standard for CUBE and ROLLUP operations
- Parse analysis ensures that SETS nodes don't contain arbitrarily deep nesting after transformation
- Example: 'GROUP BY GROUPING SETS ((a,b), CUBE(c,(d,e)))' transforms from 'SETS( RowExpr(a,b) , CUBE( c, RowExpr(d,e) ) )' to 'SETS( SIMPLE(1,2), CUBE( SIMPLE(3), SIMPLE(4,5) ) )'