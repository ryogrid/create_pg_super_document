# SerializeTransactionState

## Location
[src/backend/access/transam/xact.c:5478-5548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5478-L5548)

## Overview
SerializeTransactionState serializes the current transaction state hierarchy into a binary format that can be transmitted to parallel worker processes to maintain transaction consistency.

## Definition
```c
void SerializeTransactionState(Size maxsize, char *start_address)
```

## Detailed Description
This function writes out all relevant transaction state details needed by parallel workers into a caller-supplied buffer. It serializes:

1. Transaction isolation level (XactIsoLevel) and deferrable status (XactDeferrable)
2. Top-level and current full transaction IDs
3. Current command ID for statement-level consistency
4. All transaction IDs in the hierarchy (parent and child transactions)

The function handles two scenarios:
- If already running in a parallel worker, it passes along the existing parallel transaction ID list
- Otherwise, it builds a new sorted list of all current transaction IDs by traversing the transaction state hierarchy

All transaction IDs are emitted in sorted order for efficient processing by the receiving parallel worker process.

## Parameters / Member Variables
- `maxsize`: Maximum size of the destination buffer (should match EstimateTransactionStateSpace result)
- `start_address`: Pointer to the destination buffer where serialized state will be written

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid - validates transaction IDs before inclusion
  - [add_size](../a/add_size.md) - safe size arithmetic
  - XidFromFullTransactionId - extracts 32-bit XID from full transaction ID
  - qsort - sorts transaction IDs using xidComparator
  - [xidComparator](../x/xidComparator.md) - comparison function for transaction ID sorting
  - [palloc](../p/palloc.md) - PostgreSQL memory allocation
  - memcpy - copies transaction ID arrays
- Structures used:
  - TransactionState - transaction hierarchy traversal
  - [SerializedTransactionState](SerializedTransactionState.md) - [output](../o/output.md) structure format
  - SerializedTransactionStateHeaderSize - header size constant
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (src/backend/access/transam/parallel.c:418)

## Notes and Other Information
- Works in conjunction with EstimateTransactionStateSpace for accurate memory allocation
- Handles nested parallel worker scenarios by preserving existing parallel transaction context
- Transaction IDs are sorted to optimize lookup performance in parallel workers
- Critical for maintaining ACID properties across parallel execution boundaries
- The serialized format includes both the transaction state metadata and the complete list of active transaction IDs

## Simplified Source

```c
void SerializeTransactionState(Size maxsize, char *start_address) {
    SerializedTransactionState *result = (SerializedTransactionState *) start_address;

    // Copy basic transaction state
    result->xactIsoLevel = XactIsoLevel;
    result->xactDeferrable = XactDeferrable;
    result->topFullTransactionId = XactTopFullTransactionId;
    result->currentFullTransactionId = CurrentTransactionState->fullTransactionId;
    result->currentCommandId = currentCommandId;

    // If already in parallel worker, just pass along existing XIDs
    if (nParallelCurrentXids > 0) {
        result->nParallelCurrentXids = nParallelCurrentXids;
        memcpy(&result->parallelCurrentXids[0], ParallelCurrentXids,
               nParallelCurrentXids * sizeof(TransactionId));
        return;
    }

    // Count total XIDs in transaction hierarchy
    Size nxids = 0;
    for (TransactionState s = CurrentTransactionState; s != NULL; s = s->parent) {
        if (FullTransactionIdIsValid(s->fullTransactionId))
            nxids++;
        nxids += s->nChildXids;
    }

    // Collect all XIDs into workspace
    TransactionId *workspace = palloc(nxids * sizeof(TransactionId));
    Size i = 0;
    for (TransactionState s = CurrentTransactionState; s != NULL; s = s->parent) {
        if (FullTransactionIdIsValid(s->fullTransactionId))
            workspace[i++] = XidFromFullTransactionId(s->fullTransactionId);
        if (s->nChildXids > 0) {
            memcpy(&workspace[i], s->childXids, s->nChildXids * sizeof(TransactionId));
            i += s->nChildXids;
        }
    }

    // Sort XIDs and copy to output
    qsort(workspace, nxids, sizeof(TransactionId), xidComparator);
    result->nParallelCurrentXids = nxids;
    memcpy(&result->parallelCurrentXids[0], workspace, nxids * sizeof(TransactionId));
}
```