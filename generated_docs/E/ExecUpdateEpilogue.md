# ExecUpdateEpilogue

## Location
[src/backend/executor/nodeModifyTable.c:2153-2199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2153-L2199)

## Overview
ExecUpdateEpilogue performs the closing steps after a successful tuple update, handling index maintenance, trigger execution, and view constraint validation.

## Definition

```c
static void
ExecUpdateEpilogue(ModifyTableContext *context, UpdateContext *updateCxt,
				   ResultRelInfo *resultRelInfo, ItemPointer tupleid,
				   HeapTuple oldtuple, TupleTableSlot *slot)
```
## Detailed Description
ExecUpdateEpilogue is responsible for completing the update operation after ExecUpdateAct has successfully updated the tuple. The function performs several post-update tasks in a specific order to maintain database consistency:

1. **Index Maintenance**: Updates all relevant indexes for the modified tuple using ExecInsertIndexTuples, but only if indexes exist and updateIndexes indicates they should be maintained
2. **After Row Update Triggers**: Fires AFTER ROW UPDATE triggers, passing the appropriate transition capture context depending on whether this is part of an INSERT operation or a regular UPDATE
3. **View Constraint Checking**: Validates any WITH CHECK OPTION constraints from parent views, which must be checked after all other constraints and the physical update according to SQL specification

The function ensures that all post-update processing is completed in the correct order to maintain referential integrity and trigger semantics.

## Parameters / Member Variables
- `*context`: ModifyTableContext containing execution state and metadata
- `*updateCxt`: UpdateContext tracking update-specific information including index update requirements
- `*resultRelInfo`: ResultRelInfo for the relation that was updated
- `tupleid`: ItemPointer identifying the updated tuple
- `oldtuple`: HeapTuple containing the original tuple data before update
- `*slot`: TupleTableSlot containing the new tuple values after update
## Dependencies
- Functions called/Symbols referenced:
  - [ExecInsertIndexTuples](ExecInsertIndexTuples.md)
  - [ExecARUpdateTriggers](ExecARUpdateTriggers.md)
  - [ExecWithCheckOptions](ExecWithCheckOptions.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [ExecUpdate](ExecUpdate.md) (src/backend/executor/nodeModifyTable.c:2522)
  - [ExecMergeMatched](ExecMergeMatched.md) (src/backend/executor/nodeModifyTable.c:3071)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Index updates are conditional based on the presence of indexes and the updateIndexes flag from UpdateContext
- The function handles both regular UPDATE operations and INSERT operations with ON CONFLICT UPDATE by checking the operation type
- View constraint checking is performed last to comply with SQL specification requirements
- The recheckIndexes list is properly freed to prevent memory leaks
- AFTER ROW triggers receive both old and new tuple information for complete update context

## Simplified Source

```c
static void
ExecUpdateEpilogue(ModifyTableContext *context, UpdateContext *updateCxt,
                   ResultRelInfo *resultRelInfo, ItemPointer tupleid,
                   HeapTuple oldtuple, TupleTableSlot *slot)
{
    ModifyTableState *mtstate = context->mtstate;
    List *recheckIndexes = NIL;

    // Update index entries if table has indexes and they need updating
    if (resultRelInfo->ri_NumIndices > 0 && updateCxt->updateIndexes != TU_None) {
        recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, context->estate,
                                             true, false, NULL, NIL,
                                             (updateCxt->updateIndexes == TU_Summarizing));
    }

    // Execute AFTER ROW UPDATE triggers
    ExecARUpdateTriggers(context->estate, resultRelInfo,
                        NULL, NULL, tupleid, oldtuple, slot,
                        recheckIndexes,
                        mtstate->operation == CMD_INSERT ?
                        mtstate->mt_oc_transition_capture :
                        mtstate->mt_transition_capture,
                        false);

    // Clean up index list
    list_free(recheckIndexes);

    // Check WITH CHECK OPTION constraints from parent views
    // Must be done after all constraints and the physical update per SQL spec
    if (resultRelInfo->ri_WithCheckOptions != NIL) {
        ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo,
                           slot, context->estate);
    }
}
```