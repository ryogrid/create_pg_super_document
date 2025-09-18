# ExecInitUpdateProjection

## Location
[src/backend/executor/nodeModifyTable.c:639-696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L639-L696)

## Overview
Performs one-time initialization of projection data for UPDATE operations, setting up tuple projection to merge new column values with unchanged columns from the existing tuple.

## Definition


## Detailed Description
This function initializes the projection infrastructure required for UPDATE operations. Unlike INSERT, UPDATE always requires projection because:
1. The subplan output contains junk attributes (row identity information)
2. Updated tuples must merge new values for changed columns with existing values for unchanged columns

The function handles the complex task of creating a projection that combines:
- New values for updated columns from the subplan's output
- Existing values for non-updated columns from the old tuple
- Proper handling of column order variations in inherited UPDATE operations

Key responsibilities include:
1. **Relation Index Resolution**: Determines which result relation is being processed, handling both typical cases and inheritance scenarios
2. **Column Mapping**: Retrieves the list of columns being updated for this specific relation
3. **Slot Creation**: Creates both old and new tuple slots matching the target table format
4. **Projection Building**: Constructs an UpdateProjection that merges old and new column values appropriately

## Parameters / Member Variables
- : ModifyTable executor state containing plan information, result relation array, and execution context
- : Result relation information structure to be initialized with UPDATE projection data

## Dependencies
- Functions called/Symbols referenced:
  - outerPlan
  - [list_nth](../l/list_nth.md)
  - [table_slot_create](../t/table_slot_create.md)
  - ExecAssignExprContext
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
- Called from (representative examples):
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md)
  - [ExecUpdate](ExecUpdate.md)
  - [ExecModifyTable](ExecModifyTable.md)

## Notes and Other Information
- This is a static function only used within nodeModifyTable.c during UPDATE initialization
- Unlike INSERT, UPDATE always requires projection (no optimization for the "no projection needed" case)
- Creates both ri_oldTupleSlot and ri_newTupleSlot for the merge operation
- Handles inheritance scenarios where different result relations may have different column orders
- The whichrel calculation optimizes for the common case where mt_lastResultIndex matches the current relation
- Sets ri_projectNewInfoValid to true upon successful initialization
- The subplan evaluation flag is set to false since the subplan has already evaluated expressions