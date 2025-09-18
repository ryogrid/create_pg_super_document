# set_base_rel_consider_startup

## Location
src/backend/optimizer/path/allpaths.c: 247 - 289

## Overview
Sets the consider_param_startup flags for each base-relation entry to optimize fast-start plans for parameterized paths in SEMI and ANTI join scenarios.

## Definition


## Detailed Description
This function analyzes the join structure to identify base relations that would benefit from fast-start planning for parameterized paths. It specifically targets relations on the right-hand side (RHS) of SEMI or ANTI joins, where fast-start plans are valuable because only one tuple needs to be fetched. The function optimizes planning time by restricting this analysis to single base relations rather than complex joins.

The logic recognizes that while parameterized paths are typically used on the inside of nestloop joins (where fast-start plans have limited value), SEMI and ANTI joins present a special case where early termination makes fast-start plans beneficial.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state, including the join_info_list that describes special join conditions

## Dependencies
- Functions called/Symbols referenced:
  - SpecialJoinInfo (struct type)
  - JOIN_SEMI (enum value)
  - JOIN_ANTI (enum value) 
  - bms_get_singleton_member
  - find_base_rel
- Called from (representative examples):
  - make_one_rel

## Notes and Other Information
- Located in src/backend/optimizer/path/allpaths.c:247-289
- Static function, only used within the allpaths.c module
- Currently only handles consider_param_startup; consider_startup logic remains in build_simple_rel()
- Deliberately ignores appendrels and joinrels to minimize planning time growth
- The optimization specifically targets single base relations on RHS of SEMI/ANTI joins
- Aligns with costsize.c's costing rules for nestloop semi/antijoins