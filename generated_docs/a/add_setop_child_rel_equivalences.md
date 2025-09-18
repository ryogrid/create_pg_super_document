# add_setop_child_rel_equivalences

## Location
src/backend/optimizer/path/equivclass.c: 2883 - 2954

## Overview
Adds equivalence members for each non-resjunk target in a setop child relation's target list to the corresponding equivalence class from the setop pathkeys.

## Definition


## Detailed Description
This function is specifically designed for set operations (UNION, INTERSECT, EXCEPT) and creates equivalence class members for child relations within the set operation hierarchy. For each non-resjunk target entry in the child's target list, it adds a new equivalence member to the corresponding PathKey's equivalence class from the setop_pathkeys list. This ensures that expressions across different branches of a set operation are properly recognized as equivalent when they represent the same output column.

The function maintains the relationship between parent and child equivalence members, using the parent member's JoinDomain and establishing proper equivalence relationships. After processing all target entries, it updates the child relation's eclass_indexes to include all equivalence classes from the root planner.

## Parameters / Member Variables
- : PlannerInfo structure belonging to the top-level set operation
- : RelOptInfo of the child relation receiving new EquivalenceMembers
- : Target list for the setop child relation containing expressions to add as EquivalenceMembers
- : List of PathKeys with one entry for each non-resjunk target in child_tlist

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - lfirst_node (TargetEntry, PathKey)
  - linitial
  - add_eq_member
  - lnext
  - exprType
  - bms_add_range
  - list_length
- Called from (representative examples):
  - build_setop_child_paths

## Notes and Other Information
- Specifically designed for set operations (UNION, INTERSECT, EXCEPT)
- Skips resjunk target entries as they don't participate in set operation equivalence
- Relies on transformSetOperationStmt() ensuring no resjunk columns in targetlist
- Updates child_rel's eclass_indexes to include all root equivalence classes
- Part of PostgreSQL's set operation optimization framework
- Located in src/backend/optimizer/path/equivclass.c:2883-2954