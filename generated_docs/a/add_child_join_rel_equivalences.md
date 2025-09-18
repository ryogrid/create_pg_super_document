# add_child_join_rel_equivalences

## Location
src/backend/optimizer/path/equivclass.c: 2753 - 2882

## Overview
Creates equivalence class members for child joinrels by transforming expressions from the parent joinrel's equivalence classes.

## Definition


## Detailed Description
This function is responsible for propagating equivalence class information from parent joinrels to their child joinrels in partitioned table scenarios. It finds equivalence classes relevant to the top parent joinrel and generates transformed member expressions that reference the child joinrel instead. The function performs expression transformation using appendrel attribute adjustment, ensuring that equivalence relationships are maintained across the partition hierarchy.

The function handles both simple single-level transformations (RELOPT_JOINREL) and complex multi-level transformations (RELOPT_OTHER_JOINREL). It carefully manages memory context to avoid corruption during GEQO planning and skips volatile expressions for safety.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and equivalence class information
- : Number of AppendRelInfo structures in the appinfos array
- : Array of AppendRelInfo structures describing the parent-child relationships
- : The parent joinrel whose equivalence classes are being propagated
- : The child joinrel that will receive the transformed equivalence class members

## Dependencies
- Functions called/Symbols referenced:
  - get_eclass_indexes_for_relids
  - bms_next_member
  - list_nth
  - bms_overlap
  - bms_membership
  - adjust_appendrel_attrs
  - adjust_appendrel_attrs_multilevel
  - bms_difference
  - bms_add_members
  - add_eq_member
- Called from (representative examples):
  - build_child_join_rel

## Notes and Other Information
- Only processes equivalence classes that contain multi-relational expressions (skips single baserel expressions)
- Skips volatile equivalence classes to avoid dangerous transformations
- Uses main planner context during GEQO planning to prevent memory corruption
- Handles both simple and multi-level partition hierarchies
- Part of PostgreSQL's query optimization framework for partitioned tables
- Located in src/backend/optimizer/path/equivclass.c:2753-2882