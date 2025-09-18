# transformWithClause

## Location
src/backend/parser/parse_cte.c: 110 - 242

## Overview
Transforms the list of WITH clause "common table expressions" (CTEs) into Query nodes, handling both recursive and non-recursive WITH clauses with proper dependency management.

## Definition


## Detailed Description
This function is the main entry point for processing WITH clauses in SQL queries. It takes a parsed WITH clause and transforms all contained CTEs into their internal Query representation. The function handles two distinct cases:

1. **Recursive WITH clauses**: Performs topological sorting to eliminate forward references, builds dependency graphs, validates recursion patterns, and processes CTEs in dependency order.
2. **Non-recursive WITH clauses**: Processes CTEs sequentially, maintaining proper scoping rules where each CTE can only reference previously defined CTEs.

The function performs several critical validation steps including duplicate name checking, CTE type verification (SELECT vs. data-modifying statements), and recursion validation for recursive WITH clauses.

## Parameters / Member Variables
- : Parse state containing context information including CTE namespace and parsing flags
- : The parsed WITH clause containing the list of CTEs and recursion flag

## Dependencies
- Functions called/Symbols referenced:
  - makeDependencyGraph - builds dependency graph for recursive WITH processing
  - checkWellFormedRecursion - validates recursive CTE patterns
  - analyzeCTE - transforms individual CTEs into Query nodes
  - list_copy - creates copy of CTE list for future reference tracking
  - list_delete_first - removes processed CTEs from future list
- Called from (representative examples):
  - transformSelectStmt - when processing SELECT statements with WITH clauses
  - transformInsertStmt - when processing INSERT statements with WITH clauses
  - transformUpdateStmt - when processing UPDATE statements with WITH clauses
  - transformDeleteStmt - when processing DELETE statements with WITH clauses
  - transformMergeStmt - when processing MERGE statements with WITH clauses

## Notes and Other Information
- Only one WITH clause is allowed per query level (enforced by assertions)
- The function maintains p_ctenamespace to track visible CTEs during parsing
- For non-recursive WITH, p_future_ctes tracks not-yet-visible CTEs for better error reporting
- Data-modifying CTEs (INSERT/UPDATE/DELETE/MERGE) set the p_hasModifyingCTE flag
- All CTEs are initially marked as non-recursive and have reference count zero
- The function returns the final CTE namespace list which becomes part of the output Query