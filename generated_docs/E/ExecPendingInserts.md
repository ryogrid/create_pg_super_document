# ExecPendingInserts

## Location
src/backend/executor/nodeModifyTable.c: 1304 - 1336

## Overview
Flushes all accumulated pending batch inserts to foreign tables by iterating through the pending lists and calling ExecBatchInsert for each relation with buffered tuples.

## Definition


## Detailed Description
ExecPendingInserts serves as a batch flush mechanism for foreign table insertions. When foreign tables support batching (ri_BatchSize > 1), individual insert operations accumulate tuples in memory buffers rather than inserting them immediately. This function processes all such accumulated batches:

1. **Iterates pending lists**: Uses forboth() to simultaneously traverse both es_insert_pending_result_relations and es_insert_pending_modifytables lists
2. **Batch processing**: For each relation with pending inserts, calls ExecBatchInsert to flush the accumulated tuples
3. **Memory cleanup**: Frees the pending lists and resets them to NIL after processing all batches

The function ensures that all buffered insertions are completed before certain operations that require data visibility, such as:
- Before executing triggers that need to see inserted rows
- At the end of statement execution
- Before cross-partition updates or deletes

This design optimizes performance for foreign tables by reducing the number of round trips to external systems while maintaining correctness.

## Parameters / Member Variables
- : EState containing the pending insertion lists and execution context

## Dependencies
- Functions called/Symbols referenced:
  - forboth (macro for simultaneous list traversal)
  - [ExecBatchInsert](ExecBatchInsert.md) (performs the actual batch insertion)
  - [list_free](../l/list_free.md) (memory management)
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md) (before executing BEFORE ROW triggers)
  - [ExecDeletePrologue](ExecDeletePrologue.md) (before processing deletes)
  - [ExecUpdatePrologue](ExecUpdatePrologue.md) (before processing updates)
  - [ExecModifyTable](ExecModifyTable.md) (at statement completion)

## Notes and Other Information
- Only processes foreign table batches - regular tables don't use this mechanism
- The function safely handles empty lists (when no batches are pending)
- Both pending lists are kept in sync and must have the same length
- Memory is properly cleaned up after processing to prevent leaks
- This mechanism is crucial for maintaining MVCC visibility semantics when batching is enabled
- The function is called at strategic points to ensure data consistency requirements are met