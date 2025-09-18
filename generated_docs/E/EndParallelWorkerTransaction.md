# EndParallelWorkerTransaction

## Location
src/backend/access/transam/xact.c: 5574 - 5585

## Overview
EndParallelWorkerTransaction cleanly terminates a parallel worker transaction by committing changes and resetting the transaction state to its default condition.

## Definition
```c
void EndParallelWorkerTransaction(void)
```

## Detailed Description
This function provides the clean termination sequence for parallel worker transactions. It performs three key operations:

1. Validates that the current transaction is in the expected parallel worker state (TBLOCK_PARALLEL_INPROGRESS)
2. Commits the transaction using the standard CommitTransaction() mechanism
3. Resets the transaction block state back to TBLOCK_DEFAULT for proper cleanup

The function ensures that parallel workers properly finalize their transaction state before termination, maintaining consistency with the overall transaction management system.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - CommitTransaction - performs the actual transaction commit process
  - Assert - validates expected transaction state before proceeding
- Constants used:
  - TBLOCK_PARALLEL_INPROGRESS - expected initial state for parallel worker transactions
  - TBLOCK_DEFAULT - final state after transaction cleanup
- Structures used:
  - CurrentTransactionState - global transaction state being modified
- Called from (representative examples):
  - ParallelWorkerMain (src/backend/access/transam/parallel.c:1559)

## Notes and Other Information
- Counterpart to StartParallelWorkerTransaction - completes the parallel worker transaction lifecycle
- Must only be called on transactions that were started with StartParallelWorkerTransaction
- Uses the same CommitTransaction path as regular transactions, ensuring consistent commit processing
- Critical for proper parallel worker cleanup and resource management
- The state validation ensures that the function is only called in the appropriate context