# transformSelectStmt

## Location
[src/backend/parser/analyze.c:1337-1479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1337-L1479)

## Overview
Transforms a SELECT statement AST node into a Query tree structure, handling all SELECT-specific clauses except set operations and VALUES lists.

## Definition

```c
static Query *
transformSelectStmt(ParseState *pstate, SelectStmt *stmt)
```
## Detailed Description
transformSelectStmt is the core function responsible for converting a parsed SELECT statement (SelectStmt) into PostgreSQL's internal Query representation. This function systematically processes each component of a SELECT statement in a specific order to ensure proper dependency resolution. It handles WITH clauses, FROM clauses, target lists, WHERE conditions, HAVING conditions, GROUP BY, ORDER BY, DISTINCT, LIMIT/OFFSET, window definitions, and locking clauses.

The function performs semantic analysis and validation while building the Query tree, including type resolution, column reference validation, and aggregate function checking. It maintains parse state throughout the transformation process to track context and resolve references between different parts of the query.

Note that this function specifically handles basic SELECT statements without set operations (UNION, INTERSECT, EXCEPT) or VALUES clauses, which are handled by separate transformation functions.

## Parameters / Member Variables
- : ParseState structure containing parsing context, symbol tables, and state information
- : SelectStmt node representing the parsed SELECT statement to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (Query creation)
  - [transformWithClause](transformWithClause.md) (WITH clause processing)
  - [transformFromClause](transformFromClause.md) (FROM clause processing) 
  - [transformTargetList](transformTargetList.md) (SELECT target list processing)
  - [markTargetListOrigins](../m/markTargetListOrigins.md) (column origin tracking)
  - [transformWhereClause](transformWhereClause.md) (WHERE and HAVING clause processing)
  - [transformSortClause](transformSortClause.md) (ORDER BY processing)
  - [transformGroupClause](transformGroupClause.md) (GROUP BY processing)
  - [transformDistinctClause](transformDistinctClause.md)/transformDistinctOnClause (DISTINCT processing)
  - [transformLimitClause](transformLimitClause.md) (LIMIT/OFFSET processing)
  - [transformWindowDefinitions](transformWindowDefinitions.md) (window function processing)
  - [resolveTargetListUnknowns](../r/resolveTargetListUnknowns.md) (type resolution)
  - [makeFromExpr](../m/makeFromExpr.md) (join tree construction)
  - [transformLockingClause](transformLockingClause.md) (FOR UPDATE/SHARE processing)
  - [assign_query_collations](../a/assign_query_collations.md) (collation assignment)
  - [parseCheckAggregates](../p/parseCheckAggregates.md) (aggregate validation)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- The transformation order is critical: ORDER BY must be processed before GROUP BY and DISTINCT because they depend on the sort clause results
- The function can modify the target list during processing (passed by reference to transformation functions)
- Error handling includes validation for unsupported SELECT INTO syntax in contexts where it's not allowed
- Window definitions are processed after all window functions have been identified
- Aggregate validation is performed last after all other processing is complete
- The function sets various Query flags based on what constructs were found during parsing (hasSubLinks, hasWindowFuncs, etc.)