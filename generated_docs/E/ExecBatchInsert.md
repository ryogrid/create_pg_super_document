# ExecBatchInsert

## Location
[src/backend/executor/nodeModifyTable.c:1244-1303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1244-L1303)

## Overview
Efficiently inserts multiple tuples into a foreign table in a single batch operation, delegating the actual insertion to the Foreign Data Wrapper (FDW) and handling post-insertion triggers and constraint checks.

## Definition


## Detailed Description
ExecBatchInsert is specifically designed for foreign table batch insertion operations. It provides an efficient alternative to single-tuple insertion by allowing FDWs to process multiple tuples simultaneously. The function:

1. **Delegates to FDW**: Calls the FDW's ExecForeignBatchInsert routine to perform the actual batch insertion
2. **Handles variable insertion counts**: The FDW may insert fewer tuples than requested (returned in numInserted)
3. **Post-insertion processing**: For each successfully inserted tuple, it executes AFTER ROW INSERT triggers and validates WITH CHECK OPTION constraints
4. **Cleanup**: Clears all tuple slots and resets the batch counter for the next batch

This function is currently limited to foreign tables without RETURNING clauses, as indicated in the comment. It's called from ExecInsert when batching is enabled and from ExecPendingInserts to flush accumulated batches.

## Parameters / Member Variables
- : ModifyTableState containing the execution state and transition capture information
- : ResultRelInfo for the target foreign table
- : Array of TupleTableSlots containing the tuples to insert
- : Array of corresponding plan-level TupleTableSlots
- : Total number of slots in the batch
- : Execution state containing command tag counters and other context
- : Whether to increment the processed tuple counter

## Dependencies
- Functions called/Symbols referenced:
  - ExecForeignBatchInsert (via FDW routine - actual batch insertion)
  - [ExecARInsertTriggers](ExecARInsertTriggers.md) (AFTER ROW INSERT trigger processing)
  - [ExecWithCheckOptions](ExecWithCheckOptions.md) (WITH CHECK OPTION validation)
  - ExecClearTuple (slot cleanup)
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md) (when batch size is reached during FDW insertion)
  - [ExecPendingInserts](ExecPendingInserts.md) (to flush accumulated batches)

## Notes and Other Information
- Currently only supports foreign tables without RETURNING clauses
- The FDW may insert fewer tuples than requested, which is handled gracefully
- All successful insertions are processed for triggers and constraint checks
- Slots are cleared after processing, making them ready for reuse
- The ri_NumSlots counter is reset to 0 after batch completion
- The tableoid column is properly set for each tuple before trigger execution
- Memory cleanup is performed regardless of how many tuples were actually inserted