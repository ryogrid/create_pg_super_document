# TransactionIdAsyncCommitTree

## Location
[src/backend/access/transam/transam.c:252-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/transam.c#L252-L269)

## Overview
TransactionIdAsyncCommitTree marks a top-level transaction and all its subtransactions as committed for asynchronous commits, with commit LSN tracking.

## Definition

```c
void
TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids,
							 XLogRecPtr lsn)
```
## Detailed Description
TransactionIdAsyncCommitTree is the asynchronous commit variant of TransactionIdCommitTree. It handles the commitment of an entire transaction tree (top-level transaction plus subtransactions) for asynchronous commit operations.

The key difference from regular commits is that asynchronous commits include a specific WAL (Write-Ahead Log) record pointer (LSN) that indicates when the commit record was written to the log. This LSN is crucial for asynchronous commit semantics, as it allows the system to track when the commit record is actually flushed to durable storage.

Asynchronous commits provide better performance by not waiting for the commit record to be flushed to disk immediately, but they come with the trade-off that recent transactions might be lost in case of a crash before the WAL is flushed.

## Parameters / Member Variables
- : The top-level transaction ID to commit
- : The number of subtransaction IDs in the xids array  
- : Array of subtransaction IDs to be committed along with the main transaction
- : The LSN (Log Sequence Number) of the commit record in the WAL

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdSetTreeStatus](TransactionIdSetTreeStatus.md)
  - TRANSACTION_STATUS_COMMITTED
  - XLogRecPtr (parameter type)
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [xact_redo_commit](../x/xact_redo_commit.md)

## Notes and Other Information
- Used specifically for asynchronous commit operations where WAL flushing is deferred
- The LSN parameter is critical for determining when the transaction is truly durable
- Provides performance benefits over synchronous commits but with reduced durability guarantees
- Essential for applications that can tolerate some transaction loss in exchange for better throughput
- Used during both normal operation and WAL replay (crash recovery)
- Part of PostgreSQL's configurable commit behavior (synchronous_commit setting)
- The commit tree operation ensures all subtransactions are properly marked with the same commit semantics