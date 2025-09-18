# generate_base_implied_equalities

## Location
[src/backend/optimizer/path/equivclass.c:1028-1107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1028-L1107)

## Overview
Generates restriction clauses that can be deduced from equivalence classes, providing the foundation for equality constraint propagation throughout the query plan.

## Definition


## Detailed Description
This function is the main entry point for generating implied equality clauses from equivalence classes. It implements a sophisticated strategy that varies based on whether the EC contains constants or not:

For ECs with pseudoconstants, it generates "member = const1" clauses where const1 is the first constant member, applied to every other member. This strategy constrains all variables at their points of creation without requiring "var = var" comparisons.

For ECs without constants, it generates "member1 = member2" clauses for each pair of members belonging to the same base relation. This provides the base case for recursive constraint propagation as joins are formed.

The function also handles fallback scenarios when cross-type operators are incomplete by marking ECs as "ec_broken" and reverting to original source RestrictInfos. Additionally, it optimizes future lookups by marking base relations with their associated eclass indexes.

## Parameters / Member Variables
- : PlannerInfo structure containing the planner's global state and equivalence class information

## Dependencies
- Functions called/Symbols referenced:
  - [generate_base_implied_equalities_const](generate_base_implied_equalities_const.md)
  - [generate_base_implied_equalities_no_const](generate_base_implied_equalities_no_const.md)
  - [generate_base_implied_equalities_broken](generate_base_implied_equalities_broken.md)
  - [bms_membership](../b/bms_membership.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Sets root->ec_merging_done = true to indicate no further EC merging should occur
- Single-member ECs are skipped as they cannot generate useful deductions
- Marks base relations with has_eclass_joins = true if they can generate join clauses
- Does not attempt to avoid generating duplicate RestrictInfos for performance reasons
- Must be called after initial scanning of quals and before Path construction begins
- Falls back to ec_broken strategy when cross-type operators are incomplete
- Located in src/backend/optimizer/path/equivclass.c:1028-1107