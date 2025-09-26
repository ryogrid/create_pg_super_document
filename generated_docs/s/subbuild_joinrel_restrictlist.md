# subbuild_joinrel_restrictlist

## Location
src/backend/optimizer/util/relnode.c: 1352 - 1417

## Overview
Processes joininfo clauses from an input relation to build the restriction clause list for a new join relation.

## Definition


## Detailed Description
The  function examines each joininfo clause from an input relation and determines whether it should become a restriction clause for the new join relation. A clause becomes a restriction clause if it refers only to relations within the joinrel (i.e., no outside relations).

The function handles special logic for clone clauses, which are created during outer join processing. For clone clauses, it must verify that the clause can be safely evaluated at this join level by checking required_relids and incompatible_relids. For non-clone clauses, it asserts that the clause is properly positioned.

The function carefully eliminates duplicates using pointer equality comparison, since RestrictInfo nodes are multiply-linked rather than copied across different joinlists.

## Parameters / Member Variables
- : PlannerInfo structure containing global query planner state
- : The new join relation being constructed
- : The input relation whose joininfo clauses are being processed
- : Relids representing both inputs to the join (used for clone clause validation)
- : Existing restriction list to which new clauses will be appended

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_subset
  - RINFO_IS_PUSHED_DOWN
  - bms_overlap
  - list_append_unique_ptr
- Called from (representative examples):
  - build_joinrel_restrictlist

## Notes and Other Information
- This is a static function within relnode.c, used internally for join relation construction
- The function is part of the query optimizer's join processing logic
- Clone clauses require special handling due to outer join semantics and timing constraints
- Duplicate elimination is crucial since the same RestrictInfo nodes may appear in multiple joininfo lists
- The function operates at lines 1352-1417 in src/backend/optimizer/util/relnode.c
- Clauses that still reference outside relations remain as join clauses and are ignored by this function