# generate_base_implied_equalities_no_const

## Location
src/backend/optimizer/path/equivclass.c: 1203 - 1312

## Overview
Generates implied equality clauses for equivalence classes containing no pseudoconstants by creating "member1 = member2" restrictions between members of the same base relation.

## Definition


## Detailed Description
This function handles equivalence classes that contain only variable members (no constants or pseudoconstants). It implements a scanning strategy that tracks the last-seen member for each base relation and generates equality clauses between consecutive members of the same relation, producing the minimum number of derived clauses needed to maintain equivalence constraints.

The algorithm scans EC members once, maintaining an array of previous members indexed by relation ID. When encountering another member from the same base relation, it generates a "prev_em = cur_em" equality clause. This approach minimizes the number of generated clauses while establishing the base case for recursive constraint propagation.

Additionally, the function ensures that all variables used in member clauses will be available at any join node by adding them to the targetlist for all relations in the equivalence class, maintaining accessibility for future join operations.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and relation information
- : EquivalenceClass containing only non-constant members to process

## Dependencies
- Functions called/Symbols referenced:
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [select_equality_operator](../s/select_equality_operator.md)
  - [process_implied_equality](../p/process_implied_equality.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [generate_base_implied_equalities](generate_base_implied_equalities.md)

## Notes and Other Information
- Uses an array prev_ems to track the last-seen member for each base relation
- Generates minimum number of clauses but may fail when different orderings would succeed
- Comments suggest potential improvement using UNION-FIND algorithm similar to EC merging
- Only processes members that belong to a single base relation (singleton membership)
- Marks EC as broken (ec_broken = true) if required equality operators are unavailable
- Does not add generated clauses to ec_derives to avoid cluttering with non-join clauses
- Sets mergejoinable clause markings (left_ec, right_ec, left_em, right_em) for viable clauses
- Ensures variable availability by adding all member variables to targetlists across ec_relids
- Uses PVC_RECURSE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags
- Located in src/backend/optimizer/path/equivclass.c:1203-1312