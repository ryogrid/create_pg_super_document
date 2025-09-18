# convert_EXISTS_sublink_to_join

## Location
src/backend/optimizer/plan/subselect.c: 1371 - 1539

## Overview
Attempts to convert an EXISTS SubLink into a semi-join or anti-join, enabling the query planner to use more efficient join algorithms instead of nested loop execution for EXISTS subqueries.

## Definition


## Detailed Description
This function transforms EXISTS subqueries into semi-joins (for EXISTS) or anti-joins (for NOT EXISTS) when certain conditions are met. The transformation is a key optimization that can significantly improve query performance by allowing the optimizer to consider hash joins, merge joins, and other join algorithms instead of being limited to nested loop execution.

The function performs several validation checks before attempting the conversion:
- Ensures the subquery doesn't contain WITH clauses (CTEs)
- Verifies the subquery can be simplified using 
- Checks that the subquery body doesn't reference parent query variables
- Ensures the WHERE clause contains parent query variable references
- Validates that the WHERE clause doesn't contain volatile functions

If all checks pass, it pulls up the subquery's range table into the parent query and constructs a JoinExpr node representing the semi-join or anti-join.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and state
- : The EXISTS SubLink node to be converted
- : Boolean indicating if this is a NOT EXISTS (creates anti-join) vs EXISTS (creates semi-join)
- : Relids bitmapset of relations available for joining at this point in query planning

## Dependencies
- Functions called/Symbols referenced:
  - : Simplifies the EXISTS subquery by removing unnecessary elements
  - : Creates a deep copy of the subquery for safe modification
  - : Checks for variable references at specific query nesting levels
  - : Detects volatile function calls that prevent optimization
  - : Ensures subquery has a non-empty FROM clause
  - : Adjusts variable reference numbers after range table merger
  - : Adjusts variable sublevel references
  - : Extracts variable relation IDs from expressions
  - : Merges subquery range table into parent query
- Called from (representative examples):
  - : Main entry point for sublink pullup optimization

## Notes and Other Information
- Returns NULL if the conversion is not possible due to any validation failure
- The conversion is more restrictive than  because EXISTS subqueries must be completely flattened
- Semi-joins (EXISTS) and anti-joins (NOT EXISTS) preserve the original query semantics while enabling better join algorithms
- The function assumes the outer query has no references to the inner query, which is always true for EXISTS subqueries
- Part of PostgreSQL's subquery optimization framework that transforms correlated subqueries into joins when beneficial