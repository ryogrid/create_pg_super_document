# SimpleLruTruncate

## Location
src/backend/access/transam/slru.c: 1405 - 1499

## Overview
Removes all segments before the one holding the specified cutoff page number, safely truncating old SLRU data while handling concurrent access and I/O operations.

## Definition


## Detailed Description
SimpleLruTruncate is a maintenance function that performs safe truncation of SLRU segments containing obsolete data. It removes all segments that precede the segment containing the cutoff page, effectively reclaiming disk space from old transaction data. The function includes comprehensive safety checks to prevent wraparound bugs and ensures proper handling of concurrent I/O operations. It first cleans up the in-memory buffer pool by removing or flushing pages that precede the cutoff, then removes the corresponding disk segments. The function uses bank-based locking and includes restart logic to handle pages that are busy with I/O operations.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing configuration, callback functions, and shared state
- `cutoffPage`: The page number serving as the cutoff point; all segments before the segment containing this page will be removed

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_slru_truncate](../p/pgstat_count_slru_truncate.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - SlotGetBankNumber
  - LWLockAcquire/LWLockRelease
  - [SlruInternalWritePage](SlruInternalWritePage.md)
  - [SimpleLruWaitIO](SimpleLruWaitIO.md)
  - [SlruScanDirectory](SlruScanDirectory.md)
  - [SlruScanDirCbDeleteCutoff](SlruScanDirCbDeleteCutoff.md)
  - ereport
- Constants used:
  - SLRU_PAGE_EMPTY
  - SLRU_PAGE_VALID
- Types used:
  - SlruCtl, SlruShared
- Called from:
  - [TruncateCLOG](../T/TruncateCLOG.md)
  - [clog_redo](../c/clog_redo.md)
  - [TruncateCommitTs](../T/TruncateCommitTs.md)
  - [commit_ts_redo](../c/commit_ts_redo.md)
  - [PerformOffsetsTruncation](../P/PerformOffsetsTruncation.md)
  - [TruncateSUBTRANS](../T/TruncateSUBTRANS.md)
  - [asyncQueueAdvanceTail](../a/asyncQueueAdvanceTail.md)
  - [CheckPointPredicate](../C/CheckPointPredicate.md)

## Notes and Other Information
- Requires mutual exclusion to be established by the caller before computing cutoffPage and maintained until completion
- Includes critical safety check against wraparound bugs by verifying the latest page number doesn't precede the cutoff
- Uses restart logic when encountering I/O-busy pages, similar to SlruSelectLRUPage
- Handles dirty pages by writing them out rather than discarding, maintaining data integrity
- Bank-based locking strategy minimizes lock contention during the truncation process  
- Updates SLRU statistics counter to track truncation operations
- The function logs a warning and returns early if wraparound is detected
- Clean pages are simply marked as EMPTY, while I/O-busy pages require waiting for completion
- Final step uses SlruScanDirectory with a callback to physically remove old segment files from disk
- Typically called during or after checkpoint operations when dirty pages have already been flushed