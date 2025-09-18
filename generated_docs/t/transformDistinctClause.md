# transformDistinctClause

## Location
src/backend/parser/parse_clause.c: 2985 - 3068

## Overview
Transforms a DISTINCT clause in SQL queries by creating a list of SortGroupClause nodes that define the columns and expressions used for determining distinct rows.

## Definition


## Detailed Description
This function processes DISTINCT clauses in SQL SELECT statements and aggregate function calls. It creates a distinctClause that consists of all ORDER BY items followed by all other non-resjunk targetlist items. The function ensures that the sortClause will always be a prefix of the distinctClause, which allows PostgreSQL to absorb the sorting semantics of ORDER BY into the DISTINCT operation to avoid re-sorting.

The function enforces a critical rule: there must not be any resjunk ORDER BY items, as sorting by values that aren't necessarily unique within a DISTINCT group would make the results ill-defined. It also allows users to choose the equality semantics used by DISTINCT when working with datatypes that have multiple equality operators.

## Parameters / Member Variables
- : Parse state context containing parsing information and error handling state
- : Pointer to the query's target list, passed by reference as items may be added during processing
- : List of SortGroupClause nodes representing ORDER BY expressions
- : Boolean flag indicating if this is transforming an aggregate DISTINCT function call (affects error message phrasing only)

## Dependencies
- Functions called/Symbols referenced:
  - get_sortgroupclause_tle: Retrieves the target entry for a sort/group clause
  - copyObject: Creates a deep copy of a PostgreSQL node structure
  - addTargetToGroupList: Adds a target entry to the group list using default sort/group semantics
  - exprLocation: Gets the parse location of an expression for error reporting
  - SortGroupClause: Structure representing sort/group operations
- Called from (representative examples):
  - transformSelectStmt: Main SELECT statement transformation in analyzer
  - transformAggregateCall: Aggregate function call transformation
  - transformPLAssignStmt: PL/pgSQL assignment statement transformation

## Notes and Other Information
- The function maintains the invariant that sortClause is always a prefix of distinctClause
- Rejects queries where ORDER BY expressions don't appear in the argument list (for aggregates) or select list (for SELECT DISTINCT)
- Handles corner cases where the same target list entry appears multiple times in ORDER BY with different sort operators
- Returns an error if no columns are available to make DISTINCT, preventing malformed queries that would surprise users