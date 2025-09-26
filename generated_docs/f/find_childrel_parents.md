# find_childrel_parents

## Location
src/backend/optimizer/util/relnode.c: 1521 - 1556

## Overview
Computes the set of all parent relation IDs for an appendrel child relation, handling nested appendrel hierarchies.

## Definition


## Detailed Description
The  function traverses the appendrel hierarchy to collect all parent relation IDs for a given child relation. Since appendrels can be nested (a child relation can itself be a parent to other child relations), this function recursively walks up the hierarchy to find all ancestors.

The function starts with the given child relation and uses the append_rel_array to find its immediate parent. It adds each parent's relation ID to the result set and continues traversing upward until it reaches a base relation (RELOPT_BASEREL) that is not itself a child of another appendrel.

This is essential for correctly handling inheritance hierarchies and partitioned tables where there may be multiple levels of parent-child relationships.

## Parameters / Member Variables
- : PlannerInfo structure containing global query planning state including append_rel_array
- : The child RelOptInfo whose parent relations need to be found (must be RELOPT_OTHER_MEMBER_REL)

## Dependencies
- Functions called/Symbols referenced:
  - RELOPT_OTHER_MEMBER_REL
  - AppendRelInfo
  - bms_add_member
  - find_base_rel
  - RELOPT_BASEREL
- Called from (representative examples):
  - generate_implied_equalities_for_column
  - check_index_predicates

## Notes and Other Information
- The function asserts that the input relation is of type RELOPT_OTHER_MEMBER_REL (appendrel child)
- It uses the append_rel_array for efficient lookup of parent-child relationships
- The function handles nested appendrel structures by continuing the traversal until reaching a base relation
- The final assertion ensures that the traversal terminates at a proper base relation
- This is crucial for inheritance and partitioning scenarios where there can be multiple levels of parent-child relationships
- The function operates at lines 1521-1556 in src/backend/optimizer/util/relnode.c
- Returns a Relids bitmapset containing all parent relation IDs