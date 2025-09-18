# commit_ts_redo

## Location
src/backend/access/transam/commit_ts.c: 1023 - 1069

## Overview
The commit timestamp resource manager's WAL replay function that handles recovery of commit timestamp SLRU operations during crash recovery and standby replay.

## Definition
```c
void commit_ts_redo(XLogReaderState *record)
```

## Detailed Description
This function serves as the main WAL replay handler for the commit timestamp resource manager (RM_COMMITTS_ID). It processes WAL records related to commit timestamp operations during crash recovery, standby server replay, or other WAL replay scenarios. The function handles two main types of operations: COMMIT_TS_ZEROPAGE (for creating new zero-filled commit timestamp pages) and COMMIT_TS_TRUNCATE (for truncating old commit timestamp data). For zero page operations, it recreates the zero page and writes it to the SLRU. For truncate operations, it advances the oldest commit timestamp XID boundary and truncates the SLRU to remove old data. The function includes specific handling for XLOG replay scenarios where latest_page_number isn't properly initialized.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record to be replayed, including the operation type and associated data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecHasAnyBlockRefs
  - XLogRecGetData
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [ZeroCommitTsPage](../Z/ZeroCommitTsPage.md)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md)
  - [AdvanceOldestCommitTsXid](../A/AdvanceOldestCommitTsXid.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md)
  - COMMIT_TS_ZEROPAGE
  - COMMIT_TS_TRUNCATE
  - CommitTsCtl
- Called from (representative examples):
  - PostgreSQL WAL replay infrastructure (referenced by SizeOfCommitTsTruncate)

## Notes and Other Information
- Main entry point for commit timestamp WAL record replay
- Handles two operation types: zero page creation and SLRU truncation
- Includes assertions to verify that backup blocks are not used in commit timestamp records
- For truncate operations, includes special handling during XLOG replay to set latest_page_number appropriately
- Uses exclusive locking when replaying zero page operations to maintain consistency
- Will panic with an unknown operation code if an unrecognized WAL record type is encountered
- Part of the commit timestamp infrastructure that supports logical replication and other features requiring transaction commit time tracking
- The function ensures that commit timestamp SLRU state is properly maintained during all forms of WAL replay