# ExecInitInsertProjection

## Location
[src/backend/executor/nodeModifyTable.c:569-638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L569-L638)

## Overview
Performs one-time initialization of projection data for INSERT operations, setting up tuple projection and validation to transform subplan output into the target table format.

## Definition

```c
static void
ExecInitInsertProjection(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
```
## Detailed Description
This function initializes the projection infrastructure needed for INSERT operations. INSERT queries often need projection to filter out junk attributes (system columns, metadata) from the subplan's target list and ensure the resulting tuple matches the target table's schema.

The function performs several critical tasks:
1. **Target List Processing**: Extracts non-junk columns from the subplan's target list to create a clean insertion target list
2. **Schema Validation**: Verifies that the filtered target list produces tuples compatible with the target relation's structure  
3. **Slot Creation**: Creates a tuple slot matching the target table's format for holding new tuples
4. **Projection Setup**: Builds ProjectionInfo when junk attributes need to be filtered out

The function optimizes for the common case where no projection is needed (when the subplan output exactly matches the target table format), but handles the general case where transformation is required.

## Parameters / Member Variables
- `*mtstate`: ModifyTable executor state containing plan information and execution context
- `*resultRelInfo`: Result relation information structure to be initialized with projection data
## Dependencies
- Functions called/Symbols referenced:
  - outerPlan
  - [ExecCheckPlanOutput](ExecCheckPlanOutput.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [ExecAssignExprContext](ExecAssignExprContext.md)
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

## Simplified Source

```c
static void
ExecInitInsertProjection(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo) {
    ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    Plan *subplan = outerPlan(node);
    EState *estate = mtstate->ps.state;
    List *insertTargetList = NIL;
    bool need_projection = false;

    // Extract non-junk columns from subplan's target list
    foreach(ListCell *l, subplan->targetlist) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        if (!tle->resjunk)
            insertTargetList = lappend(insertTargetList, tle);
        else
            need_projection = true;  // Found junk columns
    }

    // Verify that target list matches the target relation schema
    ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, insertTargetList);

    // Create a slot matching the target table's format
    resultRelInfo->ri_newTupleSlot =
        table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);

    // Build projection if junk columns need to be filtered out
    if (need_projection) {
        TupleDesc relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

        // Ensure expression context exists for projection
        if (mtstate->ps.ps_ExprContext == NULL)
            ExecAssignExprContext(estate, &mtstate->ps);

        // Build the projection to transform tuples
        resultRelInfo->ri_projectNew =
            ExecBuildProjectionInfo(insertTargetList,
                                   mtstate->ps.ps_ExprContext,
                                   resultRelInfo->ri_newTupleSlot,
                                   &mtstate->ps,
                                   relDesc);
    }

    // Mark initialization as complete
    resultRelInfo->ri_projectNewInfoValid = true;
}
```