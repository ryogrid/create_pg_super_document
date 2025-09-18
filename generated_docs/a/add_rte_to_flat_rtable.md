# add_rte_to_flat_rtable

## Location
src/backend/optimizer/plan/setrefs.c: 538 - 607

## Overview
Adds a copy of a given RangeTblEntry and its corresponding RTEPermissionInfo to the flattened global rangetable while optimizing storage for executor use.

## Definition


## Detailed Description
The  function creates an optimized copy of a RangeTblEntry for inclusion in the flattened rangetable that will be used by the executor. The function performs several key operations:

**Memory Optimization**: Creates a flat copy of the RTE and zeros out substructure pointers that are not needed by the executor, reducing storage space and copying costs for cached plans. Only essential fields are preserved:
- : Common table expression name for EXPLAIN
- : Table alias for EXPLAIN  
- : Effective reference alias for EXPLAIN
- : Index to RTEPermissionInfo for executor access

**Dependency Tracking**: For relation RTEs and subqueries that were once view references (identified by valid relid), the relation OID is added to the global  list. This enables proper schema invalidation handling for cached plans, even for relations that might be unreferenced in the final plan tree.

**Permission Information Management**: If the RTE has associated permission information (), the function:
- Retrieves the existing RTEPermissionInfo from the query's rteperminfos
- Creates a new RTEPermissionInfo entry in the global finalrteperminfos
- Copies the permission data and updates the RTE's perminfoindex to reference the new global entry

## Parameters / Member Variables
- : PlannerGlobal structure containing the global state and flattened lists being built
- : List of RTEPermissionInfo structures from the current query level
- : The RangeTblEntry to be copied and added to the flattened rangetable

## Dependencies
- Functions called/Symbols referenced:
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [addRTEPermissionInfo](addRTEPermissionInfo.md)
  - lappend_oid
- Types used:
  - PlannerGlobal
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md)
- Constants used:
  - RTE_RELATION
  - RTE_SUBQUERY
- Called from (representative examples):
  - [add_rtes_to_flat_rtable](add_rtes_to_flat_rtable.md)
  - [flatten_rtes_walker](../f/flatten_rtes_walker.md)
  - fix_scan_list

## Notes and Other Information
- Zeros out numerous substructure fields including tablesample, subquery, joinaliasvars, functions, values_lists, coltypes, etc.
- Does not attempt to avoid duplicate entries in relationOids list as the cost would likely exceed the benefit
- Handles schema invalidation requirements for expanded views, eliminated child tables, and other cases
- The perminfoindex manipulation ensures proper linkage between RTEs and their permission information in the flattened structure
- Essential for preparing the rangetable for efficient executor access while maintaining necessary metadata for EXPLAIN and permissions