# RecordTransactionCommit

## Location
src/backend/access/transam/xact.c: 1304 - 1557

## Overview
RecordTransactionCommit handles the critical process of recording a transaction's commit to persistent storage, including writing commit records to WAL, managing synchronous/asynchronous commit decisions, and returning the latest transaction ID among the transaction and its children.

## Definition
```c
static TransactionId RecordTransactionCommit(void)
```

## Detailed Description
This function is the core implementation of transaction commit recording in PostgreSQL. It orchestrates the complex process of making a transaction's changes durable by writing appropriate WAL records, managing commit timestamps, handling replication origins, and making decisions about synchronous vs asynchronous commit based on various factors. The function handles both transactions with and without assigned XIDs, manages nested transactions through child XID processing, and coordinates with the checkpoint mechanism to ensure data consistency. It also handles special cases like transactions that only modified temporary tables or performed HOT pruning without requiring full commit processing.

## Parameters / Member Variables
This function takes no parameters but works with numerous local variables:
- : The top-level transaction ID obtained via GetTopTransactionIdIfAny()
- : Boolean indicating if the transaction has a valid XID to commit
- : The most recent XID among the transaction and its children (return value)
- /: Count and array of relation files pending deletion
- /: Count and array of child transaction IDs
- /: Count and array of statistics objects dropped in this transaction
- /: Count and array of invalidation messages for standby servers
- : Flag indicating if relation cache init file invalidation is needed
- : Boolean tracking whether any WAL records were written

## Dependencies
- Functions called/Symbols referenced:
  - GetTopTransactionIdIfAny
  - XLogLogicalInfoActive/LogLogicalInvalidations
  - smgrGetPendingDeletes
  - xactGetCommittedChildren
  - pgstat_get_transactional_drops
  - XLogStandbyInfoActive/xactGetCommittedInvalidationMessages
  - LogStandbyInvalidations
  - XactLogCommitRecord
  - GetCurrentTransactionStopTimestamp
  - replorigin_session_advance
  - TransactionTreeSetCommitTsData
  - XLogFlush
  - TransactionIdCommitTree
  - XLogSetAsyncXactLSN
  - TransactionIdAsyncCommitTree
  - TransactionIdLatest
  - SyncRepWaitForLSN
- Called from (representative examples):
  - CommitTransaction

## Notes and Other Information
- Critical section management prevents checkpoints from interfering with commit processing
- Supports both synchronous and asynchronous commit modes based on configuration and transaction characteristics
- Handles replication origin tracking for logical replication scenarios
- Transactions without XIDs (read-only, temp table only) receive special handling
- The function coordinates with multiple PostgreSQL subsystems: WAL, CLOG, statistics, invalidation messages, and replication
- Returns InvalidTransactionId for transactions without assigned XIDs, otherwise returns the latest XID in the transaction tree
- Essential for PostgreSQL's ACID properties and crash recovery mechanisms