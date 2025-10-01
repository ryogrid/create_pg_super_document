# transformDistinctClause

## Location
[src/backend/parser/parse_clause.c:2985-3068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L2985-L3068)

## Overview
Transforms a DISTINCT clause in SQL queries by creating a list of SortGroupClause nodes that define the columns and expressions used for determining distinct rows.

## Definition

```c
struction ensures we follow the rule
	 * that sortClause and distinctClause match;
```
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
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md): Retrieves the target entry for a sort/group clause
  - copyObject: Creates a deep copy of a PostgreSQL node structure
  - [addTargetToGroupList](../a/addTargetToGroupList.md): Adds a target entry to the group list using default sort/group semantics
  - [exprLocation](../e/exprLocation.md): Gets the parse location of an expression for error reporting
  - [SortGroupClause](../S/SortGroupClause.md): Structure representing sort/group operations
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md): Main SELECT statement transformation in analyzer
  - [transformAggregateCall](transformAggregateCall.md): Aggregate function call transformation
  - [transformPLAssignStmt](transformPLAssignStmt.md): PL/pgSQL assignment statement transformation

## Notes and Other Information
- The function maintains the invariant that sortClause is always a prefix of distinctClause
- Rejects queries where ORDER BY expressions don't appear in the argument list (for aggregates) or select list (for SELECT DISTINCT)
- Handles corner cases where the same target list entry appears multiple times in ORDER BY with different sort operators
- Returns an error if no columns are available to make DISTINCT, preventing malformed queries that would surprise users

## Simplified Source

```c
List *transformDistinctClause(ParseState *pstate, List **targetlist,
                             List *sortClause, bool is_agg) {
    List *result = NIL;

    // First, add all ORDER BY items to distinctClause
    foreach(slitem, sortClause) {
        SortGroupClause *scl = (SortGroupClause *) lfirst(slitem);
        TargetEntry *tle = get_sortgroupclause_tle(scl, *targetlist);

        // Error if ORDER BY item is not in select list
        if (tle->resjunk) {
            ereport(ERROR, /* appropriate error message for context */);
        }

        result = lappend(result, copyObject(scl));
    }

    // Add remaining non-junk targetlist items with default semantics
    foreach(tlitem, *targetlist) {
        TargetEntry *tle = (TargetEntry *) lfirst(tlitem);

        if (!tle->resjunk) {
            result = addTargetToGroupList(pstate, tle, result, *targetlist,
                                        exprLocation((Node *) tle->expr));
        }
    }

    // Error if no columns available for DISTINCT
    if (result == NIL) {
        ereport(ERROR, /* must have at least one column/argument */);
    }

    return result;
}
```