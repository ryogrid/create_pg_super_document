# SlruInternalWritePage

## Location
[src/backend/access/transam/slru.c:652-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L652-L728)

## Overview
Internal function that writes a page from a shared buffer to disk if necessary, handling synchronization and error recovery during SLRU (Simple LRU) page write operations.

## Definition


## Detailed Description
SlruInternalWritePage is a critical internal function in PostgreSQL's SLRU subsystem that manages the actual writing of dirty pages from shared memory buffers to disk storage. The function implements a sophisticated protocol for handling concurrent access, write synchronization, and error recovery.

The function performs several key operations:
1. **Concurrency Control**: Waits for any in-progress writes to complete before proceeding
2. **Dirty Page Detection**: Only writes pages that are actually dirty and still contain the expected data
3. **Write Synchronization**: Marks the page as write-in-progress to prevent concurrent modifications
4. **Lock Management**: Acquires buffer-specific locks and releases bank locks during I/O to avoid deadlocks
5. **Error Handling**: Restores dirty state and reports errors if the write operation fails
6. **Checkpoint Integration**: Updates checkpoint statistics when part of a checkpoint operation

The function is designed to be called only once per write attempt, meaning it may exit with the page still dirty if another process re-dirtied it during the write operation. However, it will attempt a fresh write even if the page is already being written, which is essential for checkpoint operations.

## Parameters / Member Variables
- : SlruCtl control structure containing SLRU configuration and state information
- : Integer slot number identifying which buffer slot contains the page to write
- : SlruWriteAll structure containing file descriptors and metadata for flush operations (can be NULL for non-flush writes)

## Dependencies
- Functions called/Symbols referenced:
  - SlotGetBankNumber
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md)  
  - [SimpleLruWaitIO](SimpleLruWaitIO.md)
  - [SlruPhysicalWritePage](SlruPhysicalWritePage.md)
  - CloseTransientFile
  - [SlruReportIOError](SlruReportIOError.md)
- Called from (representative examples):
  - [SimpleLruWritePage](SimpleLruWritePage.md)
  - [SlruSelectLRUPage](SlruSelectLRUPage.md)
  - [SimpleLruWriteAll](SimpleLruWriteAll.md)
  - [SimpleLruTruncate](SimpleLruTruncate.md)

## Notes and Other Information
- The function must be called with the appropriate bank lock held in exclusive mode
- Only performs one write attempt per call - caller must handle retry logic if needed
- Implements careful lock ordering to prevent deadlocks: bank lock → buffer lock → I/O → buffer lock → bank lock
- Part of PostgreSQL's transaction status management system, used by subsystems like CLOG, subtransaction status, and multixact
- Critical for data durability as it ensures dirty pages are persisted to disk
- Integrates with checkpoint mechanism to track buffer write statistics