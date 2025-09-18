# ExecDeletePrologue

## Location
src/backend/executor/nodeModifyTable.c: 1337 - 1368

## Overview
Performs preparatory actions for DELETE operations, primarily executing BEFORE ROW DELETE triggers and ensuring data visibility by flushing pending inserts.

## Definition


## Detailed Description
ExecDeletePrologue is a preparatory function called before the actual deletion of a tuple. Its primary responsibilities include:

1. **Initialize result status**: Sets the TM_Result to TM_Ok if a result pointer is provided
2. **Flush pending inserts**: Ensures all accumulated batch inserts are committed before trigger execution, guaranteeing that triggers see a consistent view of recently inserted data
3. **Execute BEFORE ROW triggers**: Calls ExecBRDeleteTriggersNew to fire any BEFORE DELETE triggers defined on the relation
4. **Handle trigger decisions**: Returns false if triggers determine the delete should be skipped ("do nothing"), true otherwise

The function is essential for maintaining trigger semantics and data consistency. By flushing pending inserts before trigger execution, it ensures that triggers operate on the complete current state of the database rather than a partial view missing recent batched insertions.

## Parameters / Member Variables
- : ModifyTableContext containing execution state, EPQ state, and operation metadata
- : Information about the target relation being modified
- : ItemPointer identifying the specific tuple to delete
- : HeapTuple containing the current version of the tuple being deleted
- : Output parameter for EvalPlanQual processing results
- : Output parameter for tuple modification result status

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