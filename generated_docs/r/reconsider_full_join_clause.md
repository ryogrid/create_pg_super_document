# reconsider_full_join_clause

## Location
[src/backend/optimizer/path/equivclass.c:2237-2419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2237-L2419)

## Overview
Processes a single FULL JOIN clause to determine if constant values can be propagated to both sides of the join through COALESCE expressions in equivalence classes.

## Definition
```c
static bool reconsider_full_join_clause(PlannerInfo *root, OuterJoinClauseInfo *ojcinfo)
```

## Detailed Description
This function implements optimization logic specific to FULL JOIN USING clauses by identifying cases where a merged column (represented as COALESCE(LEFTVAR, RIGHTVAR)) has been equivalenced to constants. When such equivalences exist, it can safely push LEFTVAR = CONSTANT and RIGHTVAR = CONSTANT constraints into both input relations of the full join.

The function searches through equivalence classes to find COALESCE expressions that match the full join's left and right variables. It handles the complexities of nulling relationships by stripping the full join's nulling effects from the COALESCE arguments before comparison. For each matching equivalence class containing constants, it generates equality constraints for both sides of the join.

The optimization is valid because if both sides can be constrained to the same constant values, any rows not meeting these constraints cannot contribute to the join result regardless of the full outer join semantics.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo containing global planning state and equivalence classes
- `ojcinfo`: OuterJoinClauseInfo structure containing the FULL JOIN clause details and associated special join information

## Dependencies
- Functions called/Symbols referenced:
  - [bms_make_singleton](../b/bms_make_singleton.md) (creates singleton relation bitmap for full join)
  - [is_opclause](../i/is_opclause.md), op_input_types, get_leftop, get_rightop (clause analysis functions)
  - [select_equality_operator](../s/select_equality_operator.md) (finds suitable equality operators for both sides)
  - [build_implied_join_equality](../b/build_implied_join_equality.md) (constructs new RestrictInfo clauses)
  - [find_join_domain](../f/find_join_domain.md) (locates appropriate join domains for left and right sides)
  - [process_equivalence](../p/process_equivalence.md) (processes new equivalence relationships)
  - [remove_nulling_relids](remove_nulling_relids.md) (strips nulling effects from COALESCE arguments)
  - [list_delete_nth_cell](../l/list_delete_nth_cell.md) (removes COALESCE member from equivalence class)
  - [equal](../e/equal.md) (tests expression equality)
  - foreach_current_index (tracks COALESCE member position)
- Called from (representative examples):
  - [reconsider_outer_join_clauses](reconsider_outer_join_clauses.md) (main outer join processing loop)

## Notes and Other Information
- Returns true only if both left and right variables can be successfully constrained to constants
- Specifically handles COALESCE(leftvar, rightvar) expressions created by FULL JOIN USING
- Removes the COALESCE member from the equivalence class upon successful optimization
- Requires nulling relationship handling due to FULL JOIN semantics affecting COALESCE arguments
- Currently may fail to match cross-type cases where COALESCE contains type coercion operations
- Assumes COALESCE arguments appear in the same order as the join clause variables
- Each COALESCE expression is expected to appear in at most one equivalence class
- Generates separate constraints within the appropriate JoinDomain for each side of the full join