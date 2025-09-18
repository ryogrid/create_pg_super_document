# ExecUpdateEpilogue

## Location
src/backend/executor/nodeModifyTable.c: 2153 - 2199

## Overview
ExecUpdateEpilogue performs the closing steps after a successful tuple update, handling index maintenance, trigger execution, and view constraint validation.

## Definition


## Detailed Description
ExecUpdateEpilogue is responsible for completing the update operation after ExecUpdateAct has successfully updated the tuple. The function performs several post-update tasks in a specific order to maintain database consistency:

1. **Index Maintenance**: Updates all relevant indexes for the modified tuple using ExecInsertIndexTuples, but only if indexes exist and updateIndexes indicates they should be maintained
2. **After Row Update Triggers**: Fires AFTER ROW UPDATE triggers, passing the appropriate transition capture context depending on whether this is part of an INSERT operation or a regular UPDATE
3. **View Constraint Checking**: Validates any WITH CHECK OPTION constraints from parent views, which must be checked after all other constraints and the physical update according to SQL specification

The function ensures that all post-update processing is completed in the correct order to maintain referential integrity and trigger semantics.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and metadata
- : UpdateContext tracking update-specific information including index update requirements
- : ResultRelInfo for the relation that was updated
- : ItemPointer identifying the updated tuple
- : HeapTuple containing the original tuple data before update
- : TupleTableSlot containing the new tuple values after update

## Dependencies
- Functions called/Symbols referenced:
  - ExecInsertIndexTuples
  - ExecARUpdateTriggers
  - ExecWithCheckOptions
  - list_free
- Called from (representative examples):
  - ExecUpdate (src/backend/executor/nodeModifyTable.c:2522)
  - ExecMergeMatched (src/backend/executor/nodeModifyTable.c:3071)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Index updates are conditional based on the presence of indexes and the updateIndexes flag from UpdateContext
- The function handles both regular UPDATE operations and INSERT operations with ON CONFLICT UPDATE by checking the operation type
- View constraint checking is performed last to comply with SQL specification requirements
- The recheckIndexes list is properly freed to prevent memory leaks
- AFTER ROW triggers receive both old and new tuple information for complete update context