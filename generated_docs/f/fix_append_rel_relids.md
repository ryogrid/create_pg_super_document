# fix_append_rel_relids

## Location
src/backend/optimizer/prep/prepjointree.c: 4037 - 4080

## Overview
Updates RT-index fields in AppendRelInfo nodes and their translated variables when a subquery is pulled up and relation IDs need to be remapped.

## Definition  


## Detailed Description
This function handles the updating of AppendRelInfo nodes when a subquery pullup operation requires remapping relation identifiers. It performs two main tasks:

1. **AppendRelInfo RT-index updating**: Searches through the append_rel_list to find any AppendRelInfo nodes whose child_relid matches the old varno, and updates them to use the new relation ID from subrelids.

2. **PlaceHolderVar updating**: Applies substitute_phv_relids to the translated_vars lists of AppendRelInfo nodes, since these lists might contain PlaceHolderVars that also need their relation ID references updated.

The function includes an optimization to extract the singleton member from subrelids only when needed (lazy evaluation), and includes an assertion that parent_relid should never be a pullup target, which helps catch logic errors.

The function modifies AppendRelInfo nodes in-place, which is safe in this context since they're part of the planner's working data structures.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning state and append relation list
- : The old relation ID that needs to be replaced in AppendRelInfo child_relid fields  
- : The set of relation IDs to substitute (expected to be singleton in this context)

## Dependencies
- Functions called/Symbols referenced:
  - AppendRelInfo (structure representing append relation information)
  - bms_singleton_member (extracts single member from singleton bitmapset)
  - substitute_phv_relids (updates PlaceHolderVar relation IDs)
- Called from (representative examples):
  - pull_up_simple_subquery (in prepjointree.c:1400)
  - remove_result_refs (in prepjointree.c:3812)

## Notes and Other Information
- This function is static and only used within prepjointree.c
- Part of the subquery pullup process in PostgreSQL query optimization
- Modifies AppendRelInfo nodes in-place for performance
- Includes lazy evaluation of subrelids singleton member extraction
- Contains assertion to verify parent_relid is never a pullup target (safety check)
- Only processes PlaceHolderVars if they exist in the query (lastPHId optimization)
- The function expects subrelids to be a singleton set but delays validation until actually needed
- Critical for maintaining correct relation references after inheritance or partitioning expansion during subquery pullup