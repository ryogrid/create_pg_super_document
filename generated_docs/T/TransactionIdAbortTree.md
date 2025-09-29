# TransactionIdAbortTree

## Location
[src/backend/access/transam/transam.c:270-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/transam.c#L270-L279)

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
  - [TransactionIdSetTreeStatus](TransactionIdSetTreeStatus.md)
  - TRANSACTION_STATUS_ABORTED
  - InvalidXLogRecPtr
- Called from (representative examples):
  - [RecordTransactionAbortPrepared](../R/RecordTransactionAbortPrepared.md)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md)
  - [xact_redo_abort](../x/xact_redo_abort.md)

## Notes and Other Information
- The function operates on transaction trees, ensuring that both the parent transaction and all its subtransactions are consistently marked as aborted
- The non-atomic behavior is acceptable because external observers treat uncommitted transactions uniformly regardless of their internal abort state
- This is a critical function in PostgreSQL's transaction management system, particularly during transaction abort and recovery operations

## Simplified Source

```c
void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids)
{
    TransactionIdSetTreeStatus(xid, nxids, xids,
                               TRANSACTION_STATUS_ABORTED, InvalidXLogRecPtr);
}
```

**Simplified Logic:**
1. Call `TransactionIdSetTreeStatus` with abort status for the entire transaction tree
2. Mark the top-level transaction (`xid`) as aborted
3. Mark all subtransactions in the `xids` array as aborted
4. Use `TRANSACTION_STATUS_ABORTED` status and invalid LSN

**Key Points:**
- Simple wrapper function that delegates to `TransactionIdSetTreeStatus`
- Handles entire transaction trees atomically
- Non-atomic behavior is acceptable since observers treat uncommitted transactions uniformly
- Critical for transaction abort and recovery operations