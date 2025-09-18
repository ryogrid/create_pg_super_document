# xact_redo_abort

## Location
[src/backend/access/transam/xact.c:6222-6300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L6222-L6300)

## Overview
Replays transaction abort records during WAL recovery, handling both regular transaction aborts and prepared transaction aborts while properly managing subtransactions.

## Definition


## Detailed Description
This function performs the recovery replay of transaction abort operations during PostgreSQL's crash recovery process. Unlike commit replay, abort recovery can handle subtransactions and their children, not just top-level transactions, since subtransaction aborts are WAL-logged while subtransaction commits are not. The function manages transaction ID advancement, marks transactions as aborted in pg_xact, handles known assigned transactions during hot standby, releases locks, advances replication origins, and ensures proper cleanup of relation files and statistics.

## Parameters / Member Variables
- : Parsed abort record structure containing all transaction abort information
- : Transaction ID of the aborting transaction (may be a subtransaction)
- : Log sequence number of the abort record being replayed
- : Replication origin ID for logical replication tracking

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdLatest](../T/TransactionIdLatest.md)
  - AdvanceNextFullTransactionIdPastXid
  - [TransactionIdAbortTree](../T/TransactionIdAbortTree.md)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md)
  - ExpireTreeKnownAssignedTransactionIds
  - StandbyReleaseLockTree
  - [replorigin_advance](../r/replorigin_advance.md)
  - [DropRelationFiles](../D/DropRelationFiles.md)
  - pgstat_execute_transactional_drops
  - [XLogFlush](../X/XLogFlush.md)
- Called from (representative examples):
  - [xact_redo](xact_redo.md) (for both XLOG_XACT_ABORT and XLOG_XACT_ABORT_PREPARED)

## Notes and Other Information
The function is similar to xact_redo_commit but simpler since aborts don't require invalidation message processing or complex timing considerations. A key difference is that abort records can represent subtransaction aborts (topxid != xid), unlike commits where topxid == xid always. The function includes the same WAL-first rule protection for file drops as the commit replay function. Unlike commits, aborts don't use async protocols during hot standby recovery since there are no consistency concerns with hint bits for aborted transactions.