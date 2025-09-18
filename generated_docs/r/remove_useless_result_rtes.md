# remove_useless_result_rtes

## Location
[src/backend/optimizer/prep/prepjointree.c:3427-3499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3427-L3499)

## Overview
Attempts to remove RTE_RESULT RTEs from the join tree and elides single-child FromExprs where possible, optimizing the query plan by eliminating unnecessary intermediate nodes.

## Definition
```c
void remove_useless_result_rtes(PlannerInfo *root)
```

## Detailed Description
This function performs join tree optimization by removing RTE_RESULT (result relation table entries) that contribute nothing meaningful to the query execution. RTE_RESULT entries return exactly one row and have no output columns, making them candidates for elimination when inner-joined to other relations.

The function also optimizes single-child FromExprs by replacing them with their child node where semantically valid. This is particularly important for handling outer join commutativity analysis, as it eliminates cases where the nullable side of an outer join contains a FromExpr with a single child that is another outer join.

Key optimizations performed:
- Removes RTE_RESULT entries that are inner-joined to other relations
- Handles certain outer-join cases involving RTE_RESULT
- Eliminates single-child FromExprs when their quals are empty or can be merged upward
- Handles PlaceHolderVars that reference RTE_RESULT RTEs
- Removes nulling relation references for eliminated outer joins
- Cleans up PlanRowMarks for RTE_RESULT entries

The optimization is most effective when run after expression preprocessing (which eliminates constant-true quals) and reduce_outer_joins().

## Parameters / Member Variables
- : The PlannerInfo structure containing the query tree to be optimized, including the parse tree and associated metadata

## Dependencies
- Functions called/Symbols referenced:
  - [remove_useless_results_recurse](remove_useless_results_recurse.md)
  - bms_is_empty
  - [remove_nulling_relids](remove_nulling_relids.md)
  - rt_fetch
  - foreach_delete_current
  - FromExpr (type)
  - PlanRowMark (type)
  - RTE_RESULT (constant)

- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md) (src/backend/optimizer/plan/planner.c:1117)

## Notes and Other Information
- Must be run after expression preprocessing for maximum effectiveness
- Best applied after reduce_outer_joins() since some optimizations depend on outer-join types
- The function preserves semantic correctness by carefully handling PlaceHolderVars and ensuring they have valid evaluation points
- Removes PlanRowMarks for RTE_RESULT entries to prevent executor issues with whole-row Var generation
- Part of the query optimization pipeline in PostgreSQL's planner
- Originally attempted as part of pull_up_subqueries() but separated for simplicity and effectiveness