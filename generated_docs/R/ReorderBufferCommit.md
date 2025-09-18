# ReorderBufferCommit

## Location
src/backend/replication/logical/reorderbuffer.c: 2777 - 2798

## Overview
Commits a decoded transaction by replaying its changes through the reorder buffer system.

## Definition
```c
void ReorderBufferCommit(ReorderBuffer *rb, TransactionId xid,
                        XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
                        TimestampTz commit_time,
                        RepOriginId origin_id, XLogRecPtr origin_lsn)
```

## Detailed Description
ReorderBufferCommit is a wrapper function that handles the commit of a logical replication transaction. It looks up the transaction by its transaction ID and delegates the actual replay work to ReorderBufferReplay. If the transaction is unknown (not found in the reorder buffer), the function returns early without performing any action. This function serves as the entry point for committing regular (non-prepared) transactions in the logical replication system.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing transactions
- `xid`: Transaction ID of the transaction to commit  
- `commit_lsn`: LSN where the commit record starts
- `end_lsn`: LSN where the commit record ends
- `commit_time`: Timestamp when the transaction was committed
- `origin_id`: Replication origin identifier for cross-cluster replication
- `origin_lsn`: LSN at the replication origin

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid
  - ReorderBufferReplay
- Called from (representative examples):
  - DecodeCommit

## Notes and Other Information
This function is part of the logical replication subsystem and is called during WAL replay when a commit record is encountered. It serves as a simple dispatch mechanism that locates the transaction and hands off the actual work to ReorderBufferReplay, which handles both regular commits and prepare operations.