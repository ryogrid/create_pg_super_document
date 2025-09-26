# restriction_is_always_true

## Location
[src/backend/optimizer/plan/initsplan.c:2740-2804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L2740-L2804)

## Overview
Checks whether a RestrictInfo condition is always true and can be safely removed from query evaluation. This optimization function identifies trivial conditions that don't contribute to result filtering.

## Definition

```c
bool
restriction_is_always_true(PlannerInfo *root,
						   RestrictInfo *restrictinfo)
```
## Detailed Description
The function analyzes RestrictInfo clauses to determine if they are provably always true, enabling the query planner to eliminate redundant filtering operations. Currently supports two main patterns:

1. **NullTest IS NOT NULL conditions**: Determines if an IS NOT NULL test is redundant because the expression is guaranteed to be non-null
2. **OR clauses**: Recursively checks OR branches to see if any branch is always true (making the entire OR always true)

The function includes safety checks to avoid incorrect optimizations with clone clauses, where nulling relation bits may not accurately reflect the actual nullability state.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The RestrictInfo clause to analyze for being always true

## Dependencies
- Functions called/Symbols referenced:
  - [NullTest](../N/NullTest.md) (struct type for null testing operations)
  - IS_NOT_NULL (enum value for null test type)
  - [expr_is_nonnullable](../e/expr_is_nonnullable.md) (determines if expression is guaranteed non-null)
  - [restriction_is_or_clause](restriction_is_or_clause.md) (checks if restriction is an OR clause)
  - [is_orclause](../i/is_orclause.md) (verifies if node is an OR Boolean expression)
  - [BoolExpr](../B/BoolExpr.md) (struct type for Boolean expressions)
  - [restriction_is_always_true](restriction_is_always_true.md) (recursive call for OR branch analysis)

- Called from (representative examples):
  - [add_base_clause_to_rel](../a/add_base_clause_to_rel.md) (base relation clause processing)
  - [apply_child_basequals](../a/apply_child_basequals.md) (inheritance hierarchy clause application)
  - [add_join_clause_to_rels](../a/add_join_clause_to_rels.md) (join clause distribution)

## Notes and Other Information
- Avoids optimization for clone clauses due to unreliable nulling relation information
- Skips row expressions in NullTest optimization as they can appear NULL/NOT NULL in different contexts  
- Uses recursive analysis for OR clauses - if any branch is always true, the entire OR is always true
- Part of PostgreSQL's query optimization infrastructure for eliminating redundant filter conditions
- Critical for performance as it reduces unnecessary runtime condition evaluations