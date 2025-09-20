# transformSortClause

## Location
[src/backend/parser/parse_clause.c:2732-2764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L2732-L2764)

## Overview
Transforms an ORDER BY clause into a list of SortGroupClause nodes, adding sort expressions to the targetlist as resjunk columns if needed.

## Definition

```c
List *
transformSortClause(ParseState *pstate,
					List *orderlist,
					List **targetlist,
					ParseExprKind exprKind,
					bool useSQL99)
```
## Detailed Description
This function processes ORDER BY clauses for various SQL constructs including SELECT statements, window functions, and aggregate functions. For each SortBy node in the input orderlist, it locates or creates a corresponding TargetEntry in the targetlist. The function uses different lookup strategies based on the useSQL99 flag: SQL99 rules (used for window and aggregate ORDER BY) or SQL92 rules (used for statement-level ORDER BY). After finding or creating the TargetEntry, it calls addTargetToSortList to build the final sort specification. If a sort expression is not already present in the targetlist, it gets added as a resjunk column.

## Parameters / Member Variables
- : ParseState containing parsing context and state information
- : List of SortBy nodes representing the ORDER BY expressions to transform
- : Reference to TargetEntry list where sort expressions may be added as resjunk
- : ParseExprKind enum value specifying the type of expression being parsed
- : Boolean flag determining whether to use SQL99 or SQL92 interpretation rules

## Dependencies
- Functions called/Symbols referenced:
  - [findTargetlistEntrySQL99](../f/findTargetlistEntrySQL99.md)
  - [findTargetlistEntrySQL92](../f/findTargetlistEntrySQL92.md)
  - [addTargetToSortList](../a/addTargetToSortList.md)
  - [SortBy](../S/SortBy.md) (struct type)
  - [ParseExprKind](../P/ParseExprKind.md) (enum type)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformValuesClause](transformValuesClause.md)
  - [transformSetOperationStmt](transformSetOperationStmt.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)
  - [transformAggregateCall](transformAggregateCall.md)
  - [transformWindowDefinitions](transformWindowDefinitions.md)

## Notes and Other Information
- This is a public function declared in parse_clause.h
- Used for multiple types of ORDER BY clauses: statement-level, window function, and aggregate function
- Window and aggregate ORDER BY clauses always use SQL99 rules regardless of server settings
- Automatically adds sort expressions to targetlist as resjunk columns when not already present
- The choice between SQL99 and SQL92 affects how column references and expressions are resolved
- Part of PostgreSQL's comprehensive ORDER BY support across different SQL constructs
- The resulting SortGroupClause list is used by the query planner to determine sort strategies