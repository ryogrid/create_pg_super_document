# check_index_predicates

## Location
[src/backend/optimizer/path/indxpath.c:3244-3381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3244-L3381)

## Overview
Sets the predicate-derived IndexOptInfo fields for each index of a specified relation to determine partial index usability and compute restriction info.

## Definition


## Detailed Description
This function is a crucial part of PostgreSQL's query optimizer that handles partial index predicate analysis. It determines whether partial indexes can be used by checking if the query's WHERE clauses imply the index predicates. For each index, it sets the  field to true if the predicate is satisfied and computes  - the list of restriction conditions that remain after accounting for what the index predicate already guarantees.

The function constructs a comprehensive list of available clauses including restriction clauses, movable join clauses, and equivalence-derivable join clauses. Special handling is provided for target relations (UPDATE/DELETE/MERGE/SELECT FOR UPDATE) where implied quals cannot be removed due to EvalPlanQual requirements.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query
- : RelOptInfo structure representing the relation whose indexes are being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - IS_SIMPLE_REL
  - [list_copy](../l/list_copy.md)
  - [join_clause_is_movable_to](../j/join_clause_is_movable_to.md)
  - [bms_difference](../b/bms_difference.md)
  - find_childrel_parents
  - [bms_del_members](../b/bms_del_members.md)
  - bms_is_empty
  - [list_concat](../l/list_concat.md)
  - [generate_join_implied_equalities](../g/generate_join_implied_equalities.md)
  - [bms_union](../b/bms_union.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [get_plan_rowmark](../g/get_plan_rowmark.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [contain_mutable_functions](contain_mutable_functions.md)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - RELOPT_OTHER_MEMBER_REL (constant)
- Called from (representative examples):
  - [set_plain_rel_size](../s/set_plain_rel_size.md)
  - [set_tablesample_rel_size](../s/set_tablesample_rel_size.md)

## Notes and Other Information
- Only processes base or "other" member relations (asserted via IS_SIMPLE_REL)
- Initializes indrestrictinfo to baserestrictinfo for all indexes initially
- Short-circuits if no partial indexes exist
- For target relations, leaves indrestrictinfo unchanged to ensure proper EvalPlanQual behavior
- Supports re-computation when new restrictions are added, though this rarely happens in core code
- Computes indrestrictinfo even for non-predOK indexes as they may be useful in OR clauses
- File location: src/backend/optimizer/path/indxpath.c:3244-3381