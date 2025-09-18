# restriction_is_always_false

## Location
src/backend/optimizer/plan/initsplan.c: 2805 - 2875

## Overview
Checks whether a RestrictInfo condition is always false, enabling the query planner to detect contradictory conditions that would result in empty result sets.

## Definition


## Detailed Description
The function analyzes RestrictInfo clauses to determine if they are provably always false, helping the query planner identify queries that will return no results due to contradictory conditions. This enables early query termination optimizations. Currently supports two main patterns:

1. **NullTest IS NULL conditions**: Determines if an IS NULL test is contradictory because the expression is guaranteed to be non-null
2. **OR clauses**: Checks if ALL OR branches are always false (making the entire OR always false)

Like its counterpart restriction_is_always_true, it includes safety checks to avoid incorrect optimizations with clone clauses where nulling relation information may be unreliable.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information  
- : The RestrictInfo clause to analyze for being always false

## Dependencies
- Functions called/Symbols referenced:
  - NullTest (struct type for null testing operations)
  - IS_NULL (enum value for null test type)
  - expr_is_nonnullable (determines if expression is guaranteed non-null)
  - restriction_is_or_clause (checks if restriction is an OR clause)
  - is_orclause (verifies if node is an OR Boolean expression)  
  - BoolExpr (struct type for Boolean expressions)
  - restriction_is_always_false (recursive call for OR branch analysis)

- Called from (representative examples):
  - add_base_clause_to_rel (base relation clause processing)
  - apply_child_basequals (inheritance hierarchy clause application)
  - add_join_clause_to_rels (join clause distribution)

## Notes and Other Information
- Avoids optimization for clone clauses due to unreliable nulling relation information
- Skips row expressions in NullTest optimization due to context-dependent NULL behavior
- For OR clauses, requires ALL branches to be always false (stricter than always_true case)
- Includes optimization opportunity comment about removing individual false OR branches
- Part of PostgreSQL's contradictory condition detection for early query termination
- Critical for performance as it can eliminate entire query execution when conditions are unsatisfiable