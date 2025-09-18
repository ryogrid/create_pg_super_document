# SummarizeSmgrRecord

## Location
src/backend/postmaster/walsummarizer.c: 1315 - 1363

## Overview
Handles special processing of storage manager WAL records (RM_SMGR_ID) during WAL summarization to properly track relation file creation and truncation operations.

## Definition
```c
static void SummarizeSmgrRecord(XLogReaderState *xlogreader, BlockRefTable *brtab)
```

## Detailed Description
SummarizeSmgrRecord provides specialized handling for storage manager WAL records that affect relation files in ways that require special consideration during WAL summarization. The function primarily deals with two critical storage operations: relation file creation and truncation.

For file creation (XLOG_SMGR_CREATE), the function recognizes that when a new relation fork is created on disk, tracking individual block modifications becomes unnecessary since the entire file is new. It sets the limit block to 0, effectively marking the entire relation fork as requiring full backup in incremental scenarios.

For truncation operations (XLOG_SMGR_TRUNCATE), the function handles the case where relation forks are truncated on disk. Since blocks beyond the truncation point no longer exist, there's no value in tracking modifications beyond that point. The function sets appropriate limit blocks for the affected forks (heap and visibility map) at the truncation point.

The function specifically excludes Free Space Map (FSM) fork operations since FSM is not fully WAL-logged and cannot be reliably tracked through WAL records.

## Parameters / Member Variables
- `xlogreader`: XLogReaderState containing the current storage manager WAL record being processed
- `brtab`: BlockRefTable where storage-level modifications and limits are recorded

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Extract record type information from WAL record
  - XLogRecGetData: Get the payload data from the WAL record
  - BlockRefTableSetLimitBlock: Set limit blocks for affected relation forks
  - MAIN_FORKNUM/VISIBILITYMAP_FORKNUM: Fork type identifiers
  - FSM_FORKNUM: Free Space Map fork identifier (excluded from processing)
- Called from (representative examples):
  - SummarizeWAL: Main WAL summarization loop when processing RM_SMGR_ID records

## Notes and Other Information
- Handles two specific storage manager operations: XLOG_SMGR_CREATE and XLOG_SMGR_TRUNCATE
- Ignores FSM fork operations due to incomplete WAL logging of free space maps
- For creation operations, sets limit block to 0 to indicate entire fork is new
- For truncation operations, uses the truncation point as the limit block
- Truncation handling checks flags to determine which forks are affected (heap vs visibility map)
- Critical for ensuring incremental backups handle file creation and truncation correctly
- The limit block mechanism prevents unnecessary tracking of blocks that don't need incremental processing
- SMGR_TRUNCATE_FSM is intentionally ignored since FSM modifications aren't fully WAL-logged