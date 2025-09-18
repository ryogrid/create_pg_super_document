# EvalPlanQual

## Location
src/backend/executor/execMain.c: 2472 - 2540

## Overview
Implements the EvalPlanQual mechanism to recheck modified tuples under READ COMMITTED isolation level, determining whether an updated tuple version should still be processed by the current transaction.

## Definition
TupleTableSlot *EvalPlanQual(EPQState *epqstate, Relation relation, Index rti, TupleTableSlot *inputslot)

## Detailed Description
EvalPlanQual is a core component of PostgreSQL's concurrency control system that handles tuple visibility checks under READ COMMITTED isolation. When a transaction encounters a tuple that has been modified by another committed transaction, EvalPlanQual re-evaluates the query's qualification conditions against the updated tuple version to determine if it should still be processed.

The function performs the following key steps:
1. Initializes or reinitializes the EPQ state for rechecking
2. Sets up the test tuple slot with the input tuple data
3. Marks the relation as having an available EPQ tuple for testing
4. Executes the EPQ subquery to test the updated tuple against original query conditions
5. Materializes the result tuple to ensure independence from EPQ query state
6. Cleans up the test slot and marks the relation as blocked for potential reuse

This mechanism ensures that under READ COMMITTED isolation, transactions see a consistent view of data while allowing maximum concurrency.

## Parameters / Member Variables
- `epqstate`: EPQState structure containing the state for EvalPlanQual rechecking operations
- `relation`: Relation (table) containing the tuple being checked
- `rti`: Range table index of the relation containing the tuple (must be > 0)
- `inputslot`: TupleTableSlot containing the tuple to be processed and checked

## Dependencies
- Functions called/Symbols referenced:
  - EPQState (structure type)
  - EvalPlanQualBegin
  - EvalPlanQualSlot
  - ExecCopySlot
  - EvalPlanQualNext
  - TupIsNull
  - ExecMaterializeSlot
  - ExecClearTuple
- Called from (representative examples):
  - GetTupleForTrigger
  - ExecDelete
  - ExecUpdate
  - ExecMergeMatched

## Notes and Other Information
This function is fundamental to PostgreSQL's implementation of the READ COMMITTED isolation level and snapshot-based concurrency control. The EPQ mechanism allows transactions to handle concurrent modifications gracefully by re-evaluating qualification conditions on updated tuple versions. The function typically processes tuples that have been locked with table_tuple_lock() using TUPLE_LOCK_FLAG_FIND_LAST_VERSION to ensure the input represents the latest committed version. The materialization step is crucial to prevent the returned tuple from depending on EPQ query state that might be reused or destroyed.