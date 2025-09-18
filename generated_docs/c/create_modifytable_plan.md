# create_modifytable_plan

## Location
src/backend/optimizer/plan/createplan.c: 2815 - 2855

## Overview
Creates a ModifyTable plan node for data modification operations (INSERT, UPDATE, DELETE, MERGE), configuring all necessary parameters for table modification execution.

## Definition


## Detailed Description
This function constructs a ModifyTable plan node that implements data modification operations in PostgreSQL. It handles all types of table modifications including INSERT, UPDATE, DELETE, and MERGE operations. The function creates a subplan that produces the exact target list required for the modification, applies proper labeling to maintain executor compatibility, and configures the ModifyTable node with extensive parameters needed for complex modification scenarios including partitioned tables, returning clauses, conflict resolution, and constraint checking.

The ModifyTable node is one of the most complex plan nodes in PostgreSQL due to the variety of features it must support, including triggers, constraints, returning clauses, conflict handling (UPSERT), and partitioned table operations.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context, processed target lists, and other planning information
- : ModifyTablePath representing the chosen execution strategy for the table modification, containing operation type, target relations, constraint lists, and various configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - create_plan_recurse
  - apply_tlist_labeling
  - make_modifytable
  - copy_generic_path_info
  - CP_EXACT_TLIST (flag constant)
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c module
- Requires exact target list matching using CP_EXACT_TLIST flag to ensure proper column alignment
- apply_tlist_labeling ensures result names and junk column flags are properly transferred for executor compatibility
- Handles complex scenarios including:
  - Partitioned table operations (partColsUpdated)
  - RETURNING clauses (returningLists)
  - WITH CHECK OPTION constraints (withCheckOptionLists)
  - Conflict resolution for UPSERT operations (onconflict)
  - MERGE statement operations (mergeActionLists, mergeJoinConditions)
  - Row-level security and locking (rowMarks, epqParam)
- Essential component for all data modification operations in PostgreSQL
- Supports both simple and complex modification scenarios across regular and partitioned tables