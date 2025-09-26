# subbuild_joinrel_joinlist

## Location
src/backend/optimizer/util/relnode.c: 1418 - 1469

## Overview
Processes a joininfo list to extract clauses that remain as join clauses at the current join level, filtering out those that become restriction clauses.

## Definition


## Detailed Description
The  function examines each clause in the input joininfo_list and determines whether it should remain as a join clause at the current join level. Clauses that refer only to relations within the joinrel become restriction clauses and are ignored by this function. Clauses that still reference outside relations remain as join clauses and are added to the new_joininfo list.

The function is specifically designed to work with join relations (RELOPT_JOINREL) and carefully eliminates duplicates using pointer equality, since RestrictInfo nodes are multiply-linked rather than copied across different joininfo lists.

## Parameters / Member Variables
- : The join relation being constructed, used to determine which clauses become restriction clauses
- : Input list of joininfo clauses to be processed
- : Existing joininfo list to which qualifying join clauses will be appended

## Dependencies
- Functions called/Symbols referenced:
  - RELOPT_JOINREL
  - bms_is_subset
  - list_append_unique_ptr
- Called from (representative examples):
  - build_joinrel_joinlist

## Notes and Other Information
- This is a static function within relnode.c, used internally for join relation construction
- The function asserts that it should only be called for join relations (RELOPT_JOINREL)
- Clauses that become restriction clauses are ignored since they will be handled by subbuild_joinrel_restrictlist
- Duplicate elimination uses pointer equality since RestrictInfo nodes are multiply-linked
- The function operates at lines 1418-1469 in src/backend/optimizer/util/relnode.c
- This function is the counterpart to subbuild_joinrel_restrictlist, handling the join clauses while the other handles restriction clauses