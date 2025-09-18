# ExecInitInsertProjection

## Location
src/backend/executor/nodeModifyTable.c: 569 - 638

## Overview
Performs one-time initialization of projection data for INSERT operations, setting up tuple projection and validation to transform subplan output into the target table format.

## Definition


## Detailed Description
This function initializes the projection infrastructure needed for INSERT operations. INSERT queries often need projection to filter out junk attributes (system columns, metadata) from the subplan's target list and ensure the resulting tuple matches the target table's schema.

The function performs several critical tasks:
1. **Target List Processing**: Extracts non-junk columns from the subplan's target list to create a clean insertion target list
2. **Schema Validation**: Verifies that the filtered target list produces tuples compatible with the target relation's structure  
3. **Slot Creation**: Creates a tuple slot matching the target table's format for holding new tuples
4. **Projection Setup**: Builds ProjectionInfo when junk attributes need to be filtered out

The function optimizes for the common case where no projection is needed (when the subplan output exactly matches the target table format), but handles the general case where transformation is required.

## Parameters / Member Variables
- : ModifyTable executor state containing plan information and execution context
- : Result relation information structure to be initialized with projection data

## Dependencies
- Functions called/Symbols referenced:
  - outerPlan
  - [ExecCheckPlanOutput](ExecCheckPlanOutput.md)
  - [table_slot_create](../t/table_slot_create.md)
  - ExecAssignExprContext
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
- Called from (representative examples):
  - [ExecModifyTable](ExecModifyTable.md)

## Notes and Other Information
- This is a static function only used within nodeModifyTable.c during ModifyTable node initialization
- The function sets ri_projectNewInfoValid to true to indicate successful initialization
- Projection is only built when needed (when junk attributes are present), optimizing the common case
- Creates ri_newTupleSlot to hold tuples in the target table's format
- Schema validation via ExecCheckPlanOutput ensures type compatibility between source and target
- Expression context allocation is deferred until actually needed for projection