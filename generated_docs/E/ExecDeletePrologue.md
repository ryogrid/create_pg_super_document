# ExecDeletePrologue

## Location
[src/backend/executor/nodeModifyTable.c:1337-1368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1337-L1368)

## Overview
Performs preparatory actions for DELETE operations, primarily executing BEFORE ROW DELETE triggers and ensuring data visibility by flushing pending inserts.

## Definition

```c
static bool
ExecDeletePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple,
				   TupleTableSlot **epqreturnslot, TM_Result *result)
```
## Detailed Description
ExecDeletePrologue is a preparatory function called before the actual deletion of a tuple. Its primary responsibilities include:

1. **Initialize result status**: Sets the TM_Result to TM_Ok if a result pointer is provided
2. **Flush pending inserts**: Ensures all accumulated batch inserts are committed before trigger execution, guaranteeing that triggers see a consistent view of recently inserted data
3. **Execute BEFORE ROW triggers**: Calls ExecBRDeleteTriggersNew to fire any BEFORE DELETE triggers defined on the relation
4. **Handle trigger decisions**: Returns false if triggers determine the delete should be skipped ("do nothing"), true otherwise

The function is essential for maintaining trigger semantics and data consistency. By flushing pending inserts before trigger execution, it ensures that triggers operate on the complete current state of the database rather than a partial view missing recent batched insertions.

## Parameters / Member Variables
- `*context`: ModifyTableContext containing execution state, EPQ state, and operation metadata
- `*resultRelInfo`: Information about the target relation being modified
- `tupleid`: ItemPointer identifying the specific tuple to delete
- `oldtuple`: HeapTuple containing the current version of the tuple being deleted
- `**epqreturnslot`: Output parameter for EvalPlanQual processing results
- `*result`: Output parameter for tuple modification result status
## Dependencies
- Functions called/Symbols referenced:
  - [ExecPendingInserts](ExecPendingInserts.md) (flush accumulated batch inserts)
  - [ExecBRDeleteTriggersNew](ExecBRDeleteTriggersNew.md) (execute BEFORE ROW DELETE triggers)
  - TM_Ok (tuple modification success status)
  - CMD_MERGE (command type checking)
- Called from (representative examples):
  - [ExecDelete](ExecDelete.md) (standard DELETE operation processing)
  - [ExecMergeMatched](ExecMergeMatched.md) (MERGE statement DELETE actions)

## Notes and Other Information
- The function name follows PostgreSQL's convention of "Prologue" for preparatory phases
- Pending insert flushing is critical for trigger consistency - triggers must see all committed changes
- The function handles both regular DELETE operations and DELETE actions within MERGE statements
- Return value determines whether the delete operation should proceed (true) or be skipped (false)
- The tmfd field in context is passed to trigger execution for tuple metadata handling
- [EvalPlanQual](EvalPlanQual.md) (EPQ) integration allows for concurrent update handling in higher isolation levels

## Simplified Source

```c
static bool ExecDeletePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
                              ItemPointer tupleid, HeapTuple oldtuple,
                              TupleTableSlot **epqreturnslot, TM_Result *result)
{
    // Initialize result status
    if (result)
        *result = TM_Ok;

    // Execute BEFORE ROW DELETE triggers if they exist
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_delete_before_row)
    {
        // Flush pending inserts to ensure trigger consistency
        if (context->estate->es_insert_pending_result_relations != NIL)
            ExecPendingInserts(context->estate);

        // Execute triggers and return their decision
        return ExecBRDeleteTriggersNew(context->estate, context->epqstate,
                                     resultRelInfo, tupleid, oldtuple,
                                     epqreturnslot, result, &context->tmfd,
                                     context->mtstate->operation == CMD_MERGE);
    }

    return true; // Proceed with deletion
}
```