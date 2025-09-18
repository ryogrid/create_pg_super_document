# generate_join_implied_equalities_broken

## Location
src/backend/optimizer/path/equivclass.c: 1723 - 1771

## Overview
A fallback function that recovers from EquivalenceClass processing failures by returning original RestrictInfos that are enforceable at the current join level.

## Definition
```c
static List *generate_join_implied_equalities_broken(PlannerInfo *root, EquivalenceClass *ec, Relids nominal_join_relids, Relids outer_relids, Relids nominal_inner_relids, RelOptInfo *inner_rel)
```

## Detailed Description
This function serves as a recovery mechanism when the normal equivalence class processing fails (ec_broken becomes true). Instead of trying to generate optimal join clauses from EC members, it falls back to using the original source RestrictInfos that created the equivalence class.

The function applies a filtering strategy to identify suitable RestrictInfos:
- Must be enforceable at the current join level (subset of nominal_join_relids)
- Must not be enforceable at outer relation alone
- Must not be enforceable at inner relation alone
- These conditions ensure the clause genuinely requires this specific join

For appendrel children (other relations), the function performs Var translation using adjust_appendrel_attrs_multilevel to convert parent relation variables to child relation variables, handling potentially multiple levels of inheritance.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and appendrel information
- `ec`: The broken EquivalenceClass containing source RestrictInfos to recover
- `nominal_join_relids`: Bitmap of relations involved in the join from the EC perspective
- `outer_relids`: Bitmap of relations from the outer side of the join
- `nominal_inner_relids`: Bitmap of inner relations from the EC perspective (may differ from actual inner_rel)
- `inner_rel`: RelOptInfo for the inner relation, potentially an appendrel child

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
  - IS_OTHER_REL
  - [adjust_appendrel_attrs_multilevel](../a/adjust_appendrel_attrs_multilevel.md)
- Called from (representative examples):
  - [generate_join_implied_equalities](generate_join_implied_equalities.md) (src/backend/optimizer/path/equivclass.c:1454)
  - [generate_join_implied_equalities_for_ecs](generate_join_implied_equalities_for_ecs.md) (src/backend/optimizer/path/equivclass.c:1530)

## Notes and Other Information
- Used as a last resort when equivalence class member processing fails
- Returns RestrictInfos not necessarily listed in ec_derives due to translation
- Handles complex inheritance hierarchies through multilevel attribute adjustment
- May result in less optimal join conditions compared to normal EC processing
- Critical for maintaining query correctness even when optimization fails
- Part of PostgreSQL's robust fallback strategy for equivalence class management
- Ensures that essential join conditions are not lost due to EC processing failures