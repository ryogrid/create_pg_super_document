# reconsider_outer_join_clauses

## Location
[src/backend/optimizer/path/equivclass.c:1992-2113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1992-L2113)

## Overview
Re-examines outer join clauses to derive EquivalenceClasses and create transitivity-based equality constraints that can be safely pushed to inner relations.

## Definition
```c
void reconsider_outer_join_clauses(PlannerInfo *root)
```

## Detailed Description
This function implements sophisticated optimization logic for outer join queries by analyzing deferred outer join clauses and extracting useful equality constraints. It handles three types of outer joins: LEFT JOIN, RIGHT JOIN, and FULL JOIN, each processed with specific logic.

For LEFT/RIGHT JOINs, when there's an outer join clause OUTERVAR = INNERVAR and an equivalence clause OUTERVAR = CONSTANT, it can safely push INNERVAR = CONSTANT into the inner relation's evaluation. This optimization works because inner rows not meeting this condition won't contribute to the outer join result anyway.

For FULL JOINs, it handles FULL JOIN USING scenarios where a merged column COALESCE(LEFTVAR, RIGHTVAR) has an equivalence with a constant, allowing both LEFTVAR = CONSTANT and RIGHTVAR = CONSTANT to be pushed to their respective input relations.

The function operates in a loop until no more deductions can be made, removing successfully processed clauses and replacing them with constant-TRUE dummy clauses to maintain proper join recognition during join order searching.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing the complete query planning state including the outer join clause lists to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [reconsider_outer_join_clause](reconsider_outer_join_clause.md) (processes LEFT/RIGHT join clauses)
  - [reconsider_full_join_clause](reconsider_full_join_clause.md) (processes FULL join clauses)
  - [make_restrictinfo](../m/make_restrictinfo.md) (creates new RestrictInfo structures)
  - [makeBoolConst](../m/makeBoolConst.md) (creates constant-TRUE expressions)
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md) (redistributes clauses into regular processing)
  - foreach_delete_current (removes processed clauses from lists)
  - [OuterJoinClauseInfo](../O/OuterJoinClauseInfo.md) (struct type for outer join clause information)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md) (main query planning entry point)

## Notes and Other Information
- Processes root->left_join_clauses, root->right_join_clauses, and root->full_join_clauses
- Uses iterative processing until no more deductions can be made (fixed-point algorithm)
- Replaces successfully processed outer join clauses with constant-TRUE dummy clauses to maintain join structure
- Any unprocessed clauses are thrown back into regular join processing via distribute_restrictinfo_to_rels
- The optimization is safe because it only pushes constraints that cannot affect the outer join semantics
- Critical for performance in complex queries with multiple outer joins and constant equivalences