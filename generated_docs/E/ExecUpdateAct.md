# ExecUpdateAct

## Location
[src/backend/executor/nodeModifyTable.c:2002-2152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2002-L2152)

## Overview
ExecUpdateAct is a subroutine for ExecUpdate that performs the actual tuple update operation on a plain table, handling partition constraint checks and cross-partition tuple migration when necessary.

## Definition

```c
static TM_Result
ExecUpdateAct(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
			  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
			  bool canSetTag, UpdateContext *updateCxt)
```
## Detailed Description
ExecUpdateAct is responsible for the core logic of updating a tuple in a PostgreSQL table. The function handles several critical aspects of the update operation:

1. **Generated Column Processing**: Fills in GENERATED columns using ExecUpdatePrepareSlot
2. **Partition Constraint Validation**: Checks if the updated tuple still satisfies the partition constraint
3. **Row-Level Security**: Validates RLS UPDATE WITH CHECK policies when partition constraints pass
4. **Cross-Partition Updates**: When partition constraints fail, attempts to move the tuple to the correct partition via ExecCrossPartitionUpdate
5. **Constraint Validation**: Ensures the updated tuple satisfies all table constraints
6. **Physical Update**: Performs the actual heap tuple update using table_tuple_update

The function uses a retry mechanism (via the  label) to handle cases where cross-partition updates require recomputation of GENERATED values and constraint rechecking for the destination partition.

## Parameters / Member Variables
- `*context`: ModifyTableContext containing execution state and metadata
- `*resultRelInfo`: ResultRelInfo for the target relation being updated
- `tupleid`: ItemPointer identifying the specific tuple to update
- `oldtuple`: HeapTuple containing the original tuple data
- `*slot`: TupleTableSlot containing the new tuple values
- `canSetTag`: Boolean indicating whether command tags can be set
- `*updateCxt`: UpdateContext for tracking update-specific state and results
## Dependencies
- Functions called/Symbols referenced:
  - [ExecUpdatePrepareSlot](ExecUpdatePrepareSlot.md)
  - [ExecMaterializeSlot](ExecMaterializeSlot.md)
  - [ExecPartitionCheck](ExecPartitionCheck.md)
  - [ExecWithCheckOptions](ExecWithCheckOptions.md)
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md)
  - [ExecCrossPartitionUpdateForeignKey](ExecCrossPartitionUpdateForeignKey.md)
  - [ExecConstraints](ExecConstraints.md)
  - [table_tuple_update](../t/table_tuple_update.md)
- Called from (representative examples):
  - [ExecUpdate](ExecUpdate.md) (src/backend/executor/nodeModifyTable.c:2358)
  - [ExecMergeMatched](ExecMergeMatched.md) (src/backend/executor/nodeModifyTable.c:3048)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Handles both regular updates and cross-partition updates transparently
- The retry loop (lreplace) is specifically designed for cross-partition scenarios where GENERATED values may differ between partitions
- For MERGE operations, cross-partition update retries are handled differently and delegated back to the MERGE logic
- The function integrates with PostgreSQL's tuple visibility and concurrency control mechanisms through table_tuple_update
- Foreign key constraint checking for cross-partition updates is handled via ExecCrossPartitionUpdateForeignKey

## Simplified Source

```c
static TM_Result
ExecUpdateAct(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
              ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
              bool canSetTag, UpdateContext *updateCxt)
{
    EState *estate = context->estate;
    Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
    bool partition_constraint_failed;
    TM_Result result;

    updateCxt->crossPartUpdate = false;

retry_update:
    // Fill in GENERATED columns and prepare the slot
    ExecUpdatePrepareSlot(resultRelInfo, slot, estate);
    ExecMaterializeSlot(slot);

    // Check if updated tuple still satisfies partition constraint
    partition_constraint_failed =
        resultRelationDesc->rd_rel->relispartition &&
        !ExecPartitionCheck(resultRelInfo, slot, estate, false);

    // Check RLS UPDATE policies if partition constraint passes
    if (!partition_constraint_failed &&
        resultRelInfo->ri_WithCheckOptions != NIL) {
        ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
                           resultRelInfo, slot, estate);
    }

    // Handle cross-partition update if partition constraint failed
    if (partition_constraint_failed) {
        TupleTableSlot *inserted_tuple, *retry_slot;
        ResultRelInfo *insert_destrel = NULL;

        // Attempt cross-partition update (DELETE + INSERT)
        if (ExecCrossPartitionUpdate(context, resultRelInfo,
                                   tupleid, oldtuple, slot,
                                   canSetTag, updateCxt,
                                   &result, &retry_slot,
                                   &inserted_tuple, &insert_destrel)) {
            // Success - mark as cross-partition update
            updateCxt->crossPartUpdate = true;

            // Handle foreign key triggers for cross-partition updates
            if (insert_destrel &&
                resultRelInfo->ri_TrigDesc &&
                resultRelInfo->ri_TrigDesc->trig_update_after_row) {
                ExecCrossPartitionUpdateForeignKey(context,
                                                 resultRelInfo,
                                                 insert_destrel,
                                                 tupleid, slot,
                                                 inserted_tuple);
            }

            return TM_Ok;
        }

        // Cross-partition update failed, retry needed
        if (context->mtstate->operation == CMD_MERGE)
            return result; // Let MERGE handle retry

        // Use updated tuple from retry slot and try again
        slot = retry_slot;
        goto retry_update;
    }

    // Validate remaining table constraints
    if (resultRelationDesc->rd_att->constr)
        ExecConstraints(resultRelInfo, slot, estate);

    // Perform the actual tuple update
    result = table_tuple_update(resultRelationDesc, tupleid, slot,
                              estate->es_output_cid,
                              estate->es_snapshot,
                              estate->es_crosscheck_snapshot,
                              true /* wait for commit */,
                              &context->tmfd, &updateCxt->lockmode,
                              &updateCxt->updateIndexes);

    return result;
}
```