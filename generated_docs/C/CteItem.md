# CteItem

## Location
[src/backend/parser/parse_cte.c:63-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L63-L68)

## Overview
CteItem is a utility structure used internally by PostgreSQL parser for organizing and analyzing Common Table Expression (CTE) dependencies during WITH RECURSIVE clause processing.

## Definition
```c
typedef struct CteItem
{
	CommonTableExpr *cte;		/* One CTE to examine */
	int			id;				/* Its ID number for dependencies */
	Bitmapset  *depends_on;		/* CTEs depended on (not including self) */
} CteItem;
```

## Detailed Description
CteItem is a temporary data structure used specifically during the analysis of WITH RECURSIVE clauses. When PostgreSQL encounters a WITH RECURSIVE statement, it needs to determine the proper ordering of CTEs to avoid forward references and identify which CTEs are self-referential (recursive). Rather than working directly with the list of CommonTableExprs, the parser creates an array of CteItems to facilitate dependency analysis and topological sorting.

This structure acts as a wrapper around each CommonTableExpr, adding metadata necessary for dependency tracking and recursive reference detection. It enables efficient algorithms for determining CTE evaluation order and validating recursive CTE structures.

## Parameters / Member Variables
- `cte`: Pointer to the actual CommonTableExpr that this item represents
- `id`: A unique integer identifier assigned to this CTE for dependency tracking purposes
- `depends_on`: A Bitmapset containing the IDs of other CTEs that this CTE depends on (excluding self-references to avoid circular dependencies in the analysis)

## Dependencies
- Functions called/Symbols referenced:
  - CommonTableExpr
  - [Bitmapset](../B/Bitmapset.md)
- Called from (representative examples):
  - [transformWithClause](../t/transformWithClause.md)
  - [TopologicalSort](../T/TopologicalSort.md)
  - [CteState](CteState.md) (used as member variable)

## Notes and Other Information
- This structure is only used internally during CTE parsing and analysis; it is not part of the final parsed query structure
- The `depends_on` bitmapset specifically excludes self-references to facilitate proper recursive CTE analysis
- Used primarily in the topological sorting algorithm that determines CTE evaluation order
- Part of the infrastructure that enables PostgreSQL to detect and properly handle recursive CTEs according to SQL standard requirements