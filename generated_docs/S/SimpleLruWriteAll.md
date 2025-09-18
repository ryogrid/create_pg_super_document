# SimpleLruWriteAll

## Location
src/backend/access/transam/slru.c: 1319 - 1404

## Overview
Writes all dirty pages to disk during checkpoint or database shutdown operations, implementing a coordinated flush of all modified SLRU buffer contents.

## Definition


## Detailed Description
SimpleLruWriteAll is a critical function that performs bulk write operations of all dirty pages in an SLRU buffer pool. It is typically called during checkpoint operations or database shutdown to ensure data durability. The function iterates through all buffer slots, acquires appropriate bank locks, and writes dirty pages using SlruInternalWritePage. It handles file management by tracking opened files during the write process and properly closing them afterward. The function also includes error handling for file operations and ensures directory synchronization for newly created files.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing configuration, callback functions, and shared state
- `allow_redirtied`: Boolean flag indicating whether to allow pages to be re-dirtied during the write process (typically true for checkpoints, false for shutdown)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_count_slru_flush
  - SlotGetBankNumber
  - LWLockAcquire/LWLockRelease
  - SlruInternalWritePage
  - CloseTransientFile
  - SlruReportIOError
  - fsync_fname
- Constants used:
  - SLRU_PAGE_EMPTY
  - SLRU_PAGE_VALID
  - SLRU_CLOSE_FAILED
  - SLRU_PAGES_PER_SEGMENT
  - SYNC_HANDLER_NONE
- Types used:
  - SlruCtl, SlruShared, SlruWriteAllData
- Called from:
  - CheckPointCLOG
  - CheckPointCommitTs
  - CheckPointMultiXact
  - CheckPointSUBTRANS
  - CheckPointPredicate
  - find_multixact_start

## Notes and Other Information
- Uses bank-based locking strategy to minimize lock contention during bulk writes
- Acquires and releases bank locks as it moves between different banks to reduce lock hold times
- Updates SLRU statistics counter to track flush operations
- Defers actual disk flushing until ProcessSyncRequests() is called, but synchronizes directory entries immediately
- Handles concurrent access scenarios where pages may be re-dirtied during the write process
- Includes comprehensive error handling for file close operations
- The allow_redirtied parameter accommodates different use cases: checkpoints (where concurrent activity is expected) vs. shutdown (where pages should remain clean)
- Ensures data durability by calling fsync_fname on the SLRU directory if sync handling is enabled
- Uses SlruWriteAllData structure to track file descriptors and segment numbers during the write process