# rewriteSearchAndCycle

## Location
[src/backend/rewrite/rewriteSearchCycle.c:203-681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteSearchCycle.c#L203-L681)

## Overview
Rewrites a Common Table Expression (CTE) with SEARCH or CYCLE clauses into an equivalent recursive CTE with additional columns for tracking traversal path and detecting cycles.

## Definition
```c
CommonTableExpr *rewriteSearchAndCycle(CommonTableExpr *cte)
```

## Detailed Description
This function is the main entry point for transforming recursive CTEs that have SEARCH and/or CYCLE clauses into standard recursive CTEs with appropriate path tracking mechanisms. It handles both SEARCH clauses (for breadth-first and depth-first traversal ordering) and CYCLE clauses (for cycle detection and prevention).

The function performs a comprehensive rewrite of the CTE structure:

For SEARCH clauses:
- BREADTH FIRST: Adds a search sequence column containing ROW(depth, search_columns) to track traversal depth
- DEPTH FIRST: Adds a search sequence column containing an array of ROW(search_columns) to track traversal path

For CYCLE clauses:
- Adds a cycle mark column with boolean-like values to indicate cycle detection
- Adds a cycle path column containing an array of ROW(cycle_columns) to track the path for cycle detection
- Modifies the recursive query to include a WHERE condition that prevents further traversal when a cycle is detected

The rewriting process involves:
1. Parsing the original CTE's UNION structure to identify base and recursive queries
2. Creating new left subquery (base case) with initialized path tracking columns
3. Creating new right subquery (recursive case) with path accumulation and cycle detection logic
4. Updating the SetOperationStmt and CTE metadata to include the new columns

## Parameters / Member Variables
- `cte`: Pointer to the CommonTableExpr to be rewritten, which must have either a search_clause, cycle_clause, or both

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (to create deep copies of AST nodes)
  - castNode (for type-safe casting)
  - rt_fetch (to access range table entries)
  - makeNode (to create new AST nodes)
  - [makeAlias](../m/makeAlias.md) (to create table aliases)
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md) (to adjust variable references for sublevel changes)
  - [make_path_rowexpr](../m/make_path_rowexpr.md) (to create row expressions for path tracking)
  - [make_path_initial_array](../m/make_path_initial_array.md) (to create initial path arrays)
  - [make_path_cat_expr](../m/make_path_cat_expr.md) (to create path concatenation expressions)
  - [makeVar](../m/makeVar.md), makeTargetEntry, makeFuncExpr (AST construction functions)
  - Various list manipulation functions (lappend, list_make1, etc.)
- Called from:
  - [fireRIRrules](../f/fireRIRrules.md) (in rewriteHandler.c at line 2000)

## Notes and Other Information
- This function is the core implementation of the PostgreSQL SEARCH and CYCLE clause feature for recursive CTEs
- The rewritten CTE maintains the same external interface but adds internal columns for tracking
- Supports both individual SEARCH or CYCLE clauses and combinations of both
- Includes comprehensive error checking for unsupported recursive CTE structures
- The function modifies multiple levels of the query structure including the CTE definition, subqueries, target lists, and column metadata
- [Path](../P/Path.md) tracking uses PostgreSQL's record and record array types for efficient storage and comparison
- For cycle detection, uses scalar array operations (= ANY) for efficient path membership testing