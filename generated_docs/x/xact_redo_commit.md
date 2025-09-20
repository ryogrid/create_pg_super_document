# xact_redo_commit

## Location
[src/backend/access/transam/xact.c:6068-6221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L6068-L6221)

## Overview
Replays transaction commit records during WAL recovery, handling both regular commits and prepared transaction commits with proper ordering of operations for crash recovery.

## Definition

```c
static void
xact_redo_commit(xl_xact_parsed_commit *parsed,
				 TransactionId xid,
				 XLogRecPtr lsn,
				 RepOriginId origin_id)
```
## Detailed Description
This function performs the recovery replay of transaction commit operations during PostgreSQL's crash recovery process. It carefully orchestrates multiple critical operations in the correct order including advancing transaction IDs, setting commit timestamps, updating transaction status in pg_xact, handling invalidation messages, releasing locks, advancing replication origins, dropping relation files, and executing statistical drops. The function handles both normal recovery (standbyState == STANDBY_DISABLED) and hot standby recovery with different code paths for each scenario.

## Parameters / Member Variables
- : Parsed commit record structure containing all transaction commit information
- : Transaction ID of the committing transaction
- : Log sequence number of the commit record being replayed
- : Replication origin ID for logical replication tracking

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdLatest](../T/TransactionIdLatest.md)
  - AdvanceNextFullTransactionIdPastXid
  - [TransactionTreeSetCommitTsData](../T/TransactionTreeSetCommitTsData.md)
  - [TransactionIdCommitTree](../T/TransactionIdCommitTree.md)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md)
  - [TransactionIdAsyncCommitTree](../T/TransactionIdAsyncCommitTree.md)
  - ExpireTreeKnownAssignedTransactionIds
  - [ProcessCommittedInvalidationMessages](../P/ProcessCommittedInvalidationMessages.md)
  - StandbyReleaseLockTree
  - [replorigin_advance](../r/replorigin_advance.md)
  - [DropRelationFiles](../D/DropRelationFiles.md)
  - pgstat_execute_transactional_drops
  - [XLogFlush](../X/XLogFlush.md)
  - [XLogRequestWalReceiverReply](../X/XLogRequestWalReceiverReply.md)
- Called from (representative examples):
  - [xact_redo](xact_redo.md) (for both XLOG_XACT_COMMIT and XLOG_XACT_COMMIT_PREPARED)

## Notes and Other Information
The function's execution order is critical, as noted in the comment that it was much shorter before version 9.0. During hot standby recovery, it uses async commit protocol to ensure consistency with hint bits and maintains proper ordering of clog updates before ProcArray updates. The function includes special handling for forced sync commits and apply feedback for synchronous replication scenarios. File drops and statistics operations are protected by XLogFlush calls to maintain WAL-first rule compliance.