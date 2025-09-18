# TransactionIdAbortTree

## Location
src/backend/access/transam/transam.c: 270 - 279

## Overview
Marks a given transaction and its child subtransactions as aborted in the PostgreSQL transaction status system.

## Definition
```c
void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids)
```

## Detailed Description
This function is responsible for marking a top-level transaction and all its associated subtransactions as aborted. It provides a single interface to abort an entire transaction tree by delegating to the lower-level `TransactionIdSetTreeStatus` function with the `TRANSACTION_STATUS_ABORTED` status. The function handles the non-atomic nature of the operation gracefully, as observers will consider all transactions in the tree as not-yet-committed until the abort process is complete.

## Parameters / Member Variables
- `xid`: The transaction ID of the top-level transaction to be aborted
- `nxids`: The number of subtransaction IDs in the xids array
- `xids`: Array containing the transaction IDs of committed subtransactions that should also be aborted

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdSetTreeStatus
  - TRANSACTION_STATUS_ABORTED
  - InvalidXLogRecPtr
- Called from (representative examples):
  - RecordTransactionAbortPrepared
  - RecordTransactionAbort
  - xact_redo_abort

## Notes and Other Information
- The function operates on transaction trees, ensuring that both the parent transaction and all its subtransactions are consistently marked as aborted
- The non-atomic behavior is acceptable because external observers treat uncommitted transactions uniformly regardless of their internal abort state
- This is a critical function in PostgreSQL's transaction management system, particularly during transaction abort and recovery operations