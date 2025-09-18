# RecordTransactionAbortPrepared

## Location
src/backend/access/transam/twophase.c: 2395 - 2469

## Overview
RecordTransactionAbortPrepared records the abort of a previously prepared two-phase transaction to the Write-Ahead Log and transaction status system, handling cleanup and synchronous replication.

## Definition


## Detailed Description
RecordTransactionAbortPrepared handles the abort processing for a previously prepared two-phase transaction. Similar to its commit counterpart, it writes an abort record to the WAL, marks the transaction and its subtransactions as aborted in the transaction status log (pg_xact), and performs necessary cleanup operations. The function includes safety checks to prevent aborting transactions that have already been committed, which would be a serious consistency violation. It handles replication origins appropriately and ensures synchronous replication requirements are met even for aborted transactions.

## Parameters / Member Variables
- : The transaction ID of the prepared transaction being aborted
- : Number of subtransactions that are part of this transaction
- : Array of subtransaction IDs to be aborted along with the main transaction
- : Number of relation files associated with this transaction for cleanup
- : Array of RelFileLocator structures identifying files related to the transaction
- : Number of statistics items associated with this transaction
- : Array of statistics items to be processed during abort
- : Global transaction identifier string for the prepared transaction being aborted

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - START_CRIT_SECTION
  - [XactLogAbortRecord](../X/XactLogAbortRecord.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [replorigin_session_advance](../r/replorigin_session_advance.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [TransactionIdAbortTree](../T/TransactionIdAbortTree.md)
  - END_CRIT_SECTION
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)

## Notes and Other Information
The function performs a critical safety check by verifying the transaction hasn't already been committed before proceeding with the abort, issuing a PANIC if this invariant is violated. Like prepared commits, prepared aborts cannot be optimized out since they always have at least one WAL entry. The function always flushes WAL records before removing the two-phase state file to ensure durability. It operates within a critical section for atomicity and handles both local and replicated transaction scenarios. Location: src/backend/access/transam/twophase.c:2395-2469