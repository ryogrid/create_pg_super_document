# getInsertSelectQuery

## Location
src/backend/rewrite/rewriteManip.c: 999 - 1056

## Overview
Extracts and returns the SELECT subquery from an INSERT ... SELECT construct, which is essential for applying rule transformations to the correct part of the query tree.

## Definition


## Detailed Description
This function is a specialized utility designed to handle INSERT ... SELECT constructs in rule processing contexts. When PostgreSQL processes rules, transformations often need to be applied specifically to the SELECT portion rather than the INSERT wrapper. This function identifies and extracts that SELECT subquery.

The function operates by examining the structure of the query tree:
1. First checks if this is an INSERT command
2. Looks for rule placeholder entries ("old" and "new") at the top level - if found, returns the original query
3. If placeholders aren't at top level, assumes they've been pushed down to a SELECT subquery
4. Navigates through the FROM clause to find the subquery range table entry
5. Extracts and validates the SELECT subquery, ensuring it contains the expected rule placeholders

This is specifically designed for rule-action queries where OLD and NEW placeholder entries are used to reference row values in rule processing. The function handles the case where these placeholders have been pushed down into a SELECT subquery during query transformation.

## Parameters / Member Variables
- : The Query tree to examine (expected to be an INSERT ... SELECT)
- : Optional output parameter - if provided, receives a pointer to the location of the SELECT subquery within the parse tree (useful for in-place modifications)

## Dependencies
- Functions called/Symbols referenced:
  - CMD_INSERT, CMD_SELECT (command type constants)
  - PRS2_OLD_VARNO, PRS2_NEW_VARNO (rule placeholder variable numbers)
  - rt_fetch (range table entry retrieval)
  - RangeTblRef, FromExpr (node types)
  - RTE_SUBQUERY (range table entry type)
  - list_length, linitial (list manipulation functions)
- Called from (representative examples):
  - transformRuleStmt (during rule statement parsing)
  - InsertRule, DefineQueryRewrite (during rule definition)
  - rewriteRuleAction (during rule action rewriting)
  - make_ruledef (during rule definition display)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:999-1056
- Returns the original Query if not an INSERT ... SELECT or if OLD/NEW placeholders are at top level
- Returns the SELECT subquery if it contains the rule placeholders
- Raises ERROR if the expected structure is not found
- Described in comments as "a hack" that may be cleaned up in future querytree redesigns
- Essential for correct rule processing in PostgreSQL's rewrite system
- Only applies to rule-action queries, not regular INSERT ... SELECT statements