# StartParallelWorkerTransaction

## Location
[src/backend/access/transam/xact.c:5549-5573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5549-L5573)

## Overview
StartParallelWorkerTransaction initializes a parallel worker process with the transaction state that was serialized from the main backend process, ensuring transaction consistency across parallel execution.

## Definition
```c
void StartParallelWorkerTransaction(char *tstatespace)
```

## Detailed Description
This function restores the complete transaction state in a parallel worker process by deserializing data that was previously prepared by SerializeTransactionState. The restoration process includes:

1. Starting a new transaction using StartTransaction()
2. Restoring transaction isolation level and deferrable status
3. Setting up the transaction ID hierarchy (top-level and current)
4. Restoring the current command ID for statement-level consistency
5. Installing the sorted list of parallel current transaction IDs
6. Setting the transaction block state to TBLOCK_PARALLEL_INPROGRESS

The function ensures that the parallel worker sees exactly the same transaction environment as existed in the main backend at the time of serialization.

## Parameters / Member Variables
- `tstatespace`: Pointer to the serialized transaction state buffer (created by SerializeTransactionState)

## Dependencies
- Functions called/Symbols referenced:
  - [StartTransaction](StartTransaction.md) - initializes the new transaction in the worker
  - Assert - validates that transaction state is in expected initial condition
- Structures used:
  - [SerializedTransactionState](SerializedTransactionState.md) - deserialized transaction state format
  - CurrentTransactionState - global transaction state being restored
- Constants used:
  - TBLOCK_DEFAULT - expected initial transaction block state
  - TBLOCK_PARALLEL_INPROGRESS - final block state for parallel workers
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (src/backend/access/transam/parallel.c:1455)

## Notes and Other Information
- Must be called early in parallel worker initialization before any database operations
- Requires that the worker process is in a clean initial transaction state (TBLOCK_DEFAULT)
- The parallel worker inherits the exact transaction visibility and isolation semantics of the main process
- Critical for maintaining ACID properties and consistent read behavior across parallel operations
- Works as the counterpart to SerializeTransactionState - they form a serialization/deserialization pair

## Simplified Source

```c
void StartParallelWorkerTransaction(char *tstatespace)
{
    SerializedTransactionState *tstate;

    // Verify clean initial state and start new transaction
    Assert(CurrentTransactionState->blockState == TBLOCK_DEFAULT);
    StartTransaction();

    // Deserialize and restore transaction state
    tstate = (SerializedTransactionState *) tstatespace;

    // Restore transaction properties
    XactIsoLevel = tstate->xactIsoLevel;
    XactDeferrable = tstate->xactDeferrable;

    // Restore transaction ID information
    XactTopFullTransactionId = tstate->topFullTransactionId;
    CurrentTransactionState->fullTransactionId = tstate->currentFullTransactionId;
    currentCommandId = tstate->currentCommandId;

    // Set up parallel transaction ID tracking
    nParallelCurrentXids = tstate->nParallelCurrentXids;
    ParallelCurrentXids = &tstate->parallelCurrentXids[0];

    // Mark as parallel worker transaction
    CurrentTransactionState->blockState = TBLOCK_PARALLEL_INPROGRESS;
}
```