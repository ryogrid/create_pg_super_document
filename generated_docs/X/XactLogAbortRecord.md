# XactLogAbortRecord

## Location
src/backend/access/transam/xact.c: 5924 - 6067

## Overview
Creates and writes a WAL (Write-Ahead Log) record for transaction abort operations, supporting both plain transaction aborts and two-phase commit transaction aborts.

## Definition


## Detailed Description
This function constructs and logs a comprehensive abort record to the Write-Ahead Log for transaction rollback operations. It handles both regular transaction aborts and two-phase commit prepared transaction aborts. The function collects various transaction-related metadata including sub-transactions, file relationships, dropped statistics, access exclusive locks, replication origin information, and two-phase commit details, then packages them into a structured WAL record for crash recovery and replication purposes.

## Parameters / Member Variables
- : Timestamp when the transaction abort occurred
- : Number of sub-transactions involved in this abort
- : Array of sub-transaction IDs that are being aborted
- : Number of relation file locators affected by this transaction
- : Array of RelFileLocator structures for relations modified by the transaction
- : Number of statistics items dropped during this transaction
- : Array of xl_xact_stats_item structures for dropped statistics
- : Transaction flags indicating special properties (e.g., XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK)
- : Transaction ID for two-phase commit operations (InvalidTransactionId for regular aborts)
- : Global identifier string for two-phase transactions (NULL for regular aborts)

## Dependencies
- Functions called/Symbols referenced:
  - XLogBeginInsert
  - XLogRegisterData
  - XLogSetRecordFlags
  - XLogInsert
  - XLogLogicalInfoActive
  - unconstify
- Called from (representative examples):
  - RecordTransactionAbort
  - RecordTransactionAbortPrepared

## Notes and Other Information
The function differentiates between regular transaction aborts (XLOG_XACT_ABORT) and prepared transaction aborts (XLOG_XACT_ABORT_PREPARED) based on the validity of twophase_xid. It conditionally includes various information blocks in the WAL record using xinfo flags, ensuring efficient storage by only including relevant data. The function operates within a critical section and includes replication origin information when applicable for proper logical replication support.