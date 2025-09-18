# has_dangerous_join_using

## Location
src/backend/utils/adt/ruleutils.c: 4079 - 4144

## Overview
Searches a join tree to detect unnamed JOIN USING constructs that require global column name uniqueness to avoid ambiguous references.

## Definition
static bool has_dangerous_join_using(deparse_namespace *dpns, Node *jtnode)

## Detailed Description
This function performs a recursive pre-scan of a join tree to identify problematic scenarios where unnamed (alias-less) JOIN USING constructs contain merged columns that cannot be simple references to underlying columns. When such situations exist, merged columns must be referenced as columns of the JOIN rather than input columns, but this becomes problematic for unnamed joins since there is no RTE name for qualification. The function detects when merged columns act differently from input columns due to COALESCE operations (in FULL JOINs) or implicit coercions, requiring globally unique column names across the entire query.

## Parameters / Member Variables
- dpns: Pointer to deparse_namespace structure containing the rtable for RTE lookups
- jtnode: Node representing the current position in the join tree (can be RangeTblRef, FromExpr, or JoinExpr)

## Dependencies
- Functions called/Symbols referenced:
  - RangeTblRef
  - FromExpr  
  - JoinExpr
  - rt_fetch
  - [list_nth](../l/list_nth.md)
  - nodeTag
- Called from (representative examples):
  - [set_deparse_for_query](../s/set_deparse_for_query.md)
  - [has_dangerous_join_using](has_dangerous_join_using.md) (recursive calls)

## Notes and Other Information
- Recursively traverses the join tree using different logic for each node type
- Only examines merged columns (up to joinmergedcols count) in JOIN USING RTEs
- Returns true immediately upon finding any non-Var aliasvar in merged columns
- The detection is performed before set_using_names() to determine if global uniqueness is needed
- Helps avoid unnecessary re-aliasing that would damage query readability when not required
- Handles RangeTblRef nodes as no-ops, FromExpr nodes by examining fromlist, and JoinExpr nodes by checking USING clauses and recursing on left/right arguments
- Includes error handling for unrecognized node types