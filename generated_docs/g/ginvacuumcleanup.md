# ginvacuumcleanup

## Location
[src/backend/access/gin/ginvacuum.c:688-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L688-L801)

## Overview
The vacuum cleanup function for GIN indexes that performs post-vacuum maintenance including statistics updates, free space management, and cleanup of pending insertions.

## Definition
```c
IndexBulkDeleteResult *ginvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
```

## Detailed Description
This function performs the cleanup phase of GIN index vacuum operations. It handles different scenarios based on the vacuum context: for analyze-only operations, it only cleans up pending insertions; for full vacuum operations, it performs comprehensive maintenance including page scanning, statistics collection, and free space management.

The function scans all pages in the index to identify recyclable pages, count different page types (data pages, entry pages), and collect accurate statistics. It updates the index metapage with current statistics, manages the free space map, and handles pending insertions that may have accumulated since the last vacuum. The function also properly handles locking requirements for relation extension to ensure consistent page counts.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum context including analyze_only flag and strategy
- `stats`: Existing IndexBulkDeleteResult from previous operations, or NULL for initial setup

## Dependencies
- Functions called/Symbols referenced:
  - AmAutoVacuumWorkerProcess
  - [initGinState](../i/initGinState.md)
  - [ginInsertCleanup](ginInsertCleanup.md)
  - [LockRelationForExtension](../L/LockRelationForExtension.md)
  - RelationGetNumberOfBlocks
  - [UnlockRelationForExtension](../U/UnlockRelationForExtension.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [GinPageIsRecyclable](../G/GinPageIsRecyclable.md)
  - [RecordFreeIndexPage](../R/RecordFreeIndexPage.md)
  - GinPageIsData
  - GinPageIsList
  - GinPageIsLeaf
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [ginUpdateStats](ginUpdateStats.md)
  - [IndexFreeSpaceMapVacuum](../I/IndexFreeSpaceMapVacuum.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (as part of index AM interface)

## Notes and Other Information
- Entry point for GIN index vacuum cleanup called by the vacuum subsystem
- Handles both analyze-only and full vacuum scenarios differently
- For analyze-only in autovacuum context, only cleans up pending insertions
- Scans entire index to collect accurate page and entry statistics
- Identifies and records recyclable pages for future reuse
- Updates index metapage with current statistics about pages and entries
- Manages free space map to track available space for future insertions
- Uses appropriate locking to ensure consistent relation size measurements
- Includes vacuum delay points to avoid monopolizing system resources
- Reports heap tuple count as index entry count (limitation for partial indexes)
- Part of the PostgreSQL access method interface for GIN indexes
- Ensures pending insertions are processed even when ginbulkdelete wasn't called