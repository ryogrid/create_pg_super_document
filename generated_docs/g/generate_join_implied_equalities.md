# generate_join_implied_equalities

## Location
[src/backend/optimizer/path/equivclass.c:1376-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1376-L1475)

## Overview
Generates join clauses that can be deduced from equivalence classes to enforce equality among all equivalence-class members computable at a join node.

## Definition
```c
List *generate_join_implied_equalities(PlannerInfo *root, Relids join_relids, Relids outer_relids, RelOptInfo *inner_rel, SpecialJoinInfo *sjinfo)
```

## Detailed Description
This function is a central component of PostgreSQL's equivalence class-based join optimization. It computes fresh RestrictInfo clauses for each join relation pair to ensure that all equivalence-class members computable at that node are equal. The function handles both regular joins and parameterized scans of base relations.

Key features include:
- Dynamic computation of join clauses based on the specific subset relations being joined
- Support for appendrel children with appropriate child EC member handling  
- Optimization to avoid generating duplicate clauses by reusing existing ec_sources and ec_derives
- Different processing strategies for outer joins vs inner joins
- Fallback mechanism for broken equivalence classes

The function maintains efficiency by checking for existing clauses before generating new ones and avoiding commutative duplicates. Results are suitable for merge, hash, and nestloop join methods.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and equivalence class information
- `join_relids`: Bitmap representing all relations involved in the join operation
- `outer_relids`: Bitmap of relations from the outer side of the join
- `inner_rel`: RelOptInfo structure for the inner relation being joined
- `sjinfo`: SpecialJoinInfo for outer join context, NULL for inner joins

## Dependencies
- Functions called/Symbols referenced:
  - IS_OTHER_REL
  - bms_is_empty
  - [bms_union](../b/bms_union.md)
  - [add_outer_joins_to_relids](../a/add_outer_joins_to_relids.md)
  - [get_eclass_indexes_for_relids](get_eclass_indexes_for_relids.md)
  - [get_common_eclass_indexes](get_common_eclass_indexes.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [generate_join_implied_equalities_normal](generate_join_implied_equalities_normal.md)
  - [generate_join_implied_equalities_broken](generate_join_implied_equalities_broken.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [check_index_predicates](../c/check_index_predicates.md) (src/backend/optimizer/path/indxpath.c:3311)
  - build_joinrel_restrictlist (src/backend/optimizer/util/relnode.c:1324)
  - get_baserel_parampathinfo (src/backend/optimizer/util/relnode.c:1602)

## Notes and Other Information
- Handles special case of appendrel children by using top_parent_relids for EC matching
- Uses different EC selection strategies for outer joins (all relevant ECs) vs inner joins (only common ECs)  
- Skips ECs containing constants since they don't need further enforcement
- Single-member ECs are ignored as they generate no useful join clauses
- Part of PostgreSQL's cost-based query optimizer infrastructure
- Results are cached and reused to improve planning performance for complex join trees