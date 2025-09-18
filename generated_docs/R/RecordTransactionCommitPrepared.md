# RecordTransactionCommitPrepared

## Location
src/backend/access/transam/twophase.c: 2297 - 2394

## Overview
RecordTransactionCommitPrepared records the commit of a previously prepared two-phase transaction to the Write-Ahead Log and transaction status system, handling replication origins and synchronous replication.

## Definition


## Detailed Description
RecordTransactionCommitPrepared is the final stage function for committing a prepared two-phase transaction. It writes a commit record to the WAL, marks the transaction as committed in the transaction status log (pg_xact), and handles all associated cleanup including relation file deletions, cache invalidation messages, and statistics updates. The function follows similar patterns to regular transaction commits but is specifically designed for two-phase transactions that have already been prepared. It includes special handling for replication origins when PostgreSQL is acting as a logical replication subscriber, and ensures proper synchronization with checkpointing and synchronous replication.

## Parameters / Member Variables
- : The transaction ID of the prepared transaction being committed
- : Number of subtransactions involved in this transaction
- : Array of subtransaction IDs that are part of this transaction
- : Number of relation files to be deleted as part of this commit
- : Array of RelFileLocator structures identifying files to delete
- : Number of statistics items to update
- : Array of statistics items to be updated in system catalogs
- : Number of shared invalidation messages to process
- : Array of shared invalidation messages for cache consistency
- : Boolean indicating whether to invalidate init files
- : Global transaction identifier string for the prepared transaction

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp
  - START_CRIT_SECTION
  - XactLogCommitRecord
  - replorigin_session_advance
  - TransactionTreeSetCommitTsData
  - XLogFlush
  - TransactionIdCommitTree
  - END_CRIT_SECTION
  - SyncRepWaitForLSN
- Called from (representative examples):
  - FinishPreparedTransaction

## Notes and Other Information
The function operates within a critical section to ensure atomicity and uses checkpoint delay flags to prevent race conditions during commit processing. Unlike regular commits, prepared transaction commits cannot be optimized out since they always have at least one WAL entry (the PREPARE record). The function handles both local and replicated transactions, managing commit timestamps and replication origin advancement appropriately. Location: src/backend/access/transam/twophase.c:2297-2394