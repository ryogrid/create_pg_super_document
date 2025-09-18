# SlruInternalDeleteSegment

## Location
[src/backend/access/transam/slru.c:1500-1522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1500-L1522)

## Overview
Internal function to delete an individual SLRU segment file from disk without touching the SLRU buffers.

## Definition


## Detailed Description
SlruInternalDeleteSegment performs the low-level deletion of a Single Log/Record Unit (SLRU) segment file. It handles both the cleanup of sync requests and the actual file deletion. This is an internal function that assumes the caller has already ensured that the SLRU buffers either don't contain data for this segment or have been properly cleaned out beforehand.

The function performs two main operations:
1. Forgets any pending fsync requests for the segment to avoid attempting to sync a deleted file
2. Physically unlinks (deletes) the segment file from the filesystem

## Parameters / Member Variables
- : SlruCtl structure containing SLRU control information and configuration
- : int64 segment number identifying which SLRU segment to delete

## Dependencies
- Functions called/Symbols referenced:
  - INIT_SLRUFILETAG (macro to initialize file tag)
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md) (to forget pending sync requests)
  - [SlruFileName](SlruFileName.md) (to construct the segment file path)
  - ereport (for debug logging)
  - unlink (system call to delete the file)
- Called from (representative examples):
  - [SlruDeleteSegment](SlruDeleteSegment.md)
  - [SlruScanDirCbDeleteCutoff](SlruScanDirCbDeleteCutoff.md)
  - [SlruScanDirCbDeleteAll](SlruScanDirCbDeleteAll.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside slru.c
- Callers must ensure SLRU buffers are clean before calling this function
- The function logs file deletion at DEBUG2 level for troubleshooting
- Handles sync request cleanup to maintain consistency in the sync system
- Part of PostgreSQL's SLRU (Simple Log Record Unit) subsystem used for various transaction logs