# transformDistinctOnClause

## Location
src/backend/parser/parse_clause.c: 3069 - 3175

## Overview
Transforms a DISTINCT ON clause in SQL queries by processing the specified expressions and creating a distinctClause that enforces uniqueness based on those expressions while coordinating with ORDER BY semantics.

## Definition


## Detailed Description
This function processes DISTINCT ON clauses, which allow users to specify exactly which expressions should be used to determine row uniqueness. Unlike regular DISTINCT, DISTINCT ON gives fine-grained control over which columns are considered for uniqueness testing. The function ensures that DISTINCT ON expressions are properly integrated with ORDER BY clauses, maintaining the rule that DISTINCT ON expressions must match the initial ORDER BY expressions to produce deterministic results.

The function adds all DISTINCT ON expressions to the target list (as resjunk items if not already present), assigns sortgroupref numbers to them, and creates a distinctClause that coordinates with any existing ORDER BY semantics. It enforces PostgreSQL's requirement that when both DISTINCT ON and ORDER BY are present, the ORDER BY list must begin with the DISTINCT ON expressions in the same order.

## Parameters / Member Variables
- : Parse state context containing parsing information and error handling state
- : List of expressions specified in the DISTINCT ON clause
- : Pointer to the query's target list, passed by reference as items may be added during processing
- : List of SortGroupClause nodes representing ORDER BY expressions

## Dependencies
- Functions called/Symbols referenced:
  - findTargetlistEntrySQL92: Locates or creates a target list entry for an expression
  - assignSortGroupRef: Assigns a sort group reference number to a target entry
  - lappend_int: Appends an integer value to a list
  - list_member_int: Checks if an integer value is present in a list
  - get_matching_location: Gets the parse location of a matching expression for error reporting
  - copyObject: Creates a deep copy of a PostgreSQL node structure
  - get_sortgroupref_tle: Retrieves target entry by sort group reference
  - targetIsInSortList: Checks if a target is already in the sort list
  - addTargetToGroupList: Adds a target entry to the group list using default semantics
  - SortGroupClause: Structure representing sort/group operations
  - EXPR_KIND_DISTINCT_ON: Expression kind constant for DISTINCT ON contexts
- Called from (representative examples):
  - transformSelectStmt: Main SELECT statement transformation in analyzer
  - transformPLAssignStmt: PL/pgSQL assignment statement transformation

## Notes and Other Information
- Enforces the critical rule that DISTINCT ON expressions must match initial ORDER BY expressions when both clauses are present
- Adds DISTINCT ON expressions to the target list as resjunk items if they're not already present in the select list
- Handles cases where users specify both DISTINCT ON and ORDER BY by adopting sorting semantics from matching ORDER BY items
- The implementation notes that using DISTINCT ON without proper ORDER BY coordination may produce inconsistent results
- Unlike transformDistinctClause, this function cannot return an empty result due to grammar restrictions that ensure at least one DISTINCT ON expression is always present