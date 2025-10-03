# TransactionIdCommitTree

## Location
[src/backend/access/transam/transam.c:240-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/transam.c#L240-L251)

## Overview
TransactionIdCommitTree marks a top-level transaction and all its subtransactions as committed in a single operation.

## Definition

```c
void
TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids)
```
## Detailed Description
TransactionIdCommitTree is a high-level transaction management function that commits an entire transaction tree (a top-level transaction and all its subtransactions) atomically. It serves as a wrapper around the lower-level TransactionIdSetTreeStatus function, specifically for commit operations.

The function is designed to handle complex transaction hierarchies where a main transaction may have spawned multiple subtransactions. When the main transaction commits, all its subtransactions must also be marked as committed to maintain consistency.

The commit operation follows PostgreSQL's transaction logging protocol where subtransactions are first marked as subcommitted, then the top-level transaction is committed, making the entire tree visible to other transactions.

## Parameters / Member Variables
- `xid`: The top-level transaction ID to commit
- `nxids`: The number of subtransaction IDs in the xids array
- `*xids`: Array of subtransaction IDs to be committed along with the main transaction
## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdSetTreeStatus](TransactionIdSetTreeStatus.md)
  - TRANSACTION_STATUS_COMMITTED
  - InvalidXLogRecPtr
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [RecordTransactionCommitPrepared](../R/RecordTransactionCommitPrepared.md)
  - [xact_redo_commit](../x/xact_redo_commit.md)

## Notes and Other Information
- This operation is not guaranteed to be atomic at the individual transaction level, but subtransactions are correctly marked first
- Used during normal transaction commit and prepared transaction commit scenarios  
- Critical for maintaining transaction hierarchy consistency in PostgreSQL's MVCC system
- The InvalidXLogRecPtr parameter indicates this is a regular commit (not an async commit with specific LSN)
- Part of the transaction commit pipeline that ensures all related transactions are properly marked in the commit log
- Essential for two-phase commit protocols and transaction recovery during crash recovery

## Simplified Source

```c
void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids) {
    // Mark the entire transaction tree as committed
    // Use InvalidXLogRecPtr since this is a regular (not async) commit
    TransactionIdSetTreeStatus(xid, nxids, xids,
                               TRANSACTION_STATUS_COMMITTED,
                               InvalidXLogRecPtr);
}
```