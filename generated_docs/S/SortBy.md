# SortBy

## Location
[src/include/nodes/parsenodes.h:543-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L543-L551)

## Overview
SortBy is a parse tree node that represents a single sorting specification in ORDER BY clauses, capturing the expression to sort on along with sorting direction and null handling preferences.

## Definition

```c
typedef struct SortBy
{
	NodeTag		type;
	Node	   *node;			/* expression to sort on */
	SortByDir	sortby_dir;		/* ASC/DESC/USING/default */
	SortByNulls sortby_nulls;	/* NULLS FIRST/LAST */
	List	   *useOp;			/* name of op to use, if SORTBY_USING */
	ParseLoc	location;		/* operator location, or -1 if none/unknown */
} SortBy;
```
## Detailed Description
SortBy nodes are fundamental components of PostgreSQL's ORDER BY clause processing. Each SortBy node represents one sorting key in an ORDER BY clause, containing the expression to sort on, the sorting direction (ASC/DESC), null value ordering preferences (NULLS FIRST/LAST), and optional custom operator specifications for advanced sorting. The structure supports both standard sorting (ASC/DESC) and custom operator-based sorting (USING clause). These nodes are created during parsing and later transformed into execution plan sorting specifications.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a SortBy node
- `*node`: Pointer to the expression that should be evaluated for sorting (can be column references, function calls, etc.)
- `sortby_dir`: Enumeration specifying sort direction - ASC (ascending), DESC (descending), USING (custom operator), or default
- `sortby_nulls`: Enumeration specifying how NULL values should be ordered - NULLS FIRST or NULLS LAST
- `*useOp`: List containing the name of the operator to use when sortby_dir is SORTBY_USING, NULL otherwise
- `location`: Parse location of the sort operator in the original query text, or -1 if unknown
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited structure member)
  - [Node](../N/Node.md) (base type for sort expression)
  - [SortByDir](SortByDir.md) (enumeration for sort direction)
  - SortByNulls (enumeration for null ordering)
  - [List](../L/List.md) (for operator names in USING clause)
  - ParseLoc (for source location tracking)
- Called from (representative examples):
  - [transformSortClause](../t/transformSortClause.md) (src/backend/parser/parse_clause.c:2743)
  - [addTargetToSortList](../a/addTargetToSortList.md) (src/backend/parser/parse_clause.c:3394)
  - [transformAggregateCall](../t/transformAggregateCall.md) (src/backend/parser/parse_agg.c:139)
  - transformFuncCall (src/backend/parser/parse_expr.c:1466)

## Notes and Other Information
- [SortBy](SortBy.md) nodes are created during SQL parsing and are later processed to generate sort specifications for query execution
- The useOp field is only populated when using custom sorting operators with the USING clause
- Location tracking helps provide meaningful error messages when sort specifications are invalid
- [SortBy](SortBy.md) supports both simple column sorting and complex expression-based sorting
- File location: src/include/nodes/parsenodes.h:543-551