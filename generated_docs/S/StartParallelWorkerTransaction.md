# StartParallelWorkerTransaction

## Location
src/backend/access/transam/xact.c: 5549 - 5573

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
  - StartTransaction - initializes the new transaction in the worker
  - Assert - validates that transaction state is in expected initial condition
- Structures used:
  - SerializedTransactionState - deserialized transaction state format
  - CurrentTransactionState - global transaction state being restored
- Constants used:
  - TBLOCK_DEFAULT - expected initial transaction block state
  - TBLOCK_PARALLEL_INPROGRESS - final block state for parallel workers
- Called from (representative examples):
  - ParallelWorkerMain (src/backend/access/transam/parallel.c:1455)

## Notes and Other Information
- Must be called early in parallel worker initialization before any database operations
- Requires that the worker process is in a clean initial transaction state (TBLOCK_DEFAULT)
- The parallel worker inherits the exact transaction visibility and isolation semantics of the main process
- Critical for maintaining ACID properties and consistent read behavior across parallel operations
- Works as the counterpart to SerializeTransactionState - they form a serialization/deserialization pair