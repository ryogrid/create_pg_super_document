# ExecInsert

## Location
[src/backend/executor/nodeModifyTable.c:779-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L779-L1243)

## Overview
Handles the insertion of a single tuple into a table (or partition thereof) and its associated indexes, supporting complex scenarios like foreign tables, batch inserts, ON CONFLICT handling, and RETURNING clauses.

## Definition

```c
static TupleTableSlot *
ExecInsert(ModifyTableContext *context,
		   ResultRelInfo *resultRelInfo,
		   TupleTableSlot *slot,
		   bool canSetTag,
		   TupleTableSlot **inserted_tuple,
		   ResultRelInfo **insert_destrel)
```
## Detailed Description
ExecInsert is the core function responsible for inserting tuples in PostgreSQL's executor. It handles multiple insertion scenarios:

1. **Partition routing**: For partitioned tables, it finds the appropriate leaf partition to insert into
2. **Foreign table handling**: Delegates to FDW routines for foreign tables, including batch insertion support
3. **Regular table insertion**: Performs standard heap insertion with index maintenance
4. **Constraint validation**: Validates RLS policies, CHECK constraints, and partition constraints
5. **Conflict resolution**: Implements INSERT ... ON CONFLICT DO NOTHING/UPDATE logic using speculative insertion
6. **Trigger processing**: Executes BEFORE ROW, INSTEAD OF, and AFTER ROW triggers
7. **Generated columns**: Computes stored generated columns before insertion

The function supports batching for FDWs that can handle multiple rows efficiently. For ON CONFLICT scenarios, it uses speculative insertion to minimize rollback overhead when conflicts occur.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and plan information
- : Information about the target relation for insertion  
- : TupleTableSlot containing the tuple values to be inserted
- : Whether the command tag counter should be incremented
- : Output parameter returning the effectively inserted tuple
- : Output parameter returning the relation where insertion occurred

## Dependencies
- Functions called/Symbols referenced:
  - [ExecPrepareTupleRouting](ExecPrepareTupleRouting.md) (partition routing)
  - ExecMaterializeSlot (tuple materialization)
  - [ExecBRInsertTriggers](ExecBRInsertTriggers.md)/ExecIRInsertTriggers/ExecARInsertTriggers (trigger handling)
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md) (generated columns)
  - [ExecBatchInsert](ExecBatchInsert.md) (FDW batch processing)
  - [ExecCheckIndexConstraints](ExecCheckIndexConstraints.md) (conflict detection)
  - [ExecOnConflictUpdate](ExecOnConflictUpdate.md) (ON CONFLICT DO UPDATE)
  - table_tuple_insert/table_tuple_insert_speculative (heap insertion)
  - [ExecInsertIndexTuples](ExecInsertIndexTuples.md) (index maintenance)
  - [ExecProcessReturning](ExecProcessReturning.md) (RETURNING clause processing)
- Called from (representative examples):
  - [ExecModifyTable](ExecModifyTable.md) (main INSERT execution)
  - [ExecMergeNotMatched](ExecMergeNotMatched.md) (MERGE statement INSERT actions)
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md) (partition key updates)

## Notes and Other Information
- The function may change the active tuple conversion map in mtstate->mt_transition_capture, requiring callers to save the previous value
- For FDW batch insertion, tuples are accumulated in ri_Slots until the batch size is reached
- Speculative insertion is used for ON CONFLICT to avoid expensive rollbacks on conflicts
- The function handles both regular and cross-partition insertions (when a tuple is moved between partitions during UPDATE)
- Memory contexts are carefully managed, especially for batch operations to avoid excessive memory usage
- The function returns NULL for "do nothing" cases or when batching (actual insertion is deferred)