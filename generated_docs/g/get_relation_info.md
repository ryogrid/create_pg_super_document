# get_relation_info

## Location
src/backend/optimizer/util/plancat.c: 116 - 589

## Overview
Retrieves comprehensive catalog information for a given relation and populates the RelOptInfo structure with metadata needed for query planning and optimization.

## Definition


## Detailed Description
This function serves as the primary interface for gathering all relation metadata required by the PostgreSQL query planner. It opens the specified relation and extracts essential information including attribute ranges, index details, statistics, foreign key relationships, and table access method capabilities.

The function handles different relation types (regular tables, foreign tables, partitioned tables) and populates the RelOptInfo structure with:
- Attribute boundaries (min_attr, max_attr) and NOT NULL constraints
- Size estimates (pages, tuples, allvisfrac) via estimate_rel_size
- Index information through IndexOptInfo structures
- Foreign table metadata (server ID, FDW routines)
- Parallel worker configuration
- Extended statistics objects
- Foreign key relationships

Special handling is provided for inheritance scenarios - when inhparent is true, the function focuses on attribute setup since the RelOptInfo represents an appendrel formed by an inheritance tree rather than a physical relation.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state
- : OID of the relation to analyze and gather information for
- : Boolean indicating if this relation is an inheritance parent (affects processing scope)
- : RelOptInfo structure to populate with the gathered relation metadata

## Dependencies
- Functions called/Symbols referenced:
  - table_open, table_close
  - [estimate_rel_size](../e/estimate_rel_size.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [index_open](../i/index_open.md), index_close
  - [get_relation_foreign_keys](get_relation_foreign_keys.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - [build_index_tlist](../b/build_index_tlist.md)
- Called from (representative examples):
  - [build_simple_rel](../b/build_simple_rel.md)

## Notes and Other Information
- Requires the relation to already be locked by the rewriter or expand_inherited_rtentry()
- Validates relation accessibility during recovery for temporary/unlogged relations
- Handles both regular and partitioned indexes with special processing for btree indexes
- Uses a plugin hook (get_relation_info_hook) to allow external modification of gathered information
- Maintains backward compatibility by using lcons() instead of lappend() for index list construction