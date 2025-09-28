# SimpleLruWriteAll

## Location
[src/backend/access/transam/slru.c:1319-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1319-L1404)

## Overview
Writes all dirty pages to disk during checkpoint or database shutdown operations, implementing a coordinated flush of all modified SLRU buffer contents.

## Definition

```c
void
SimpleLruWriteAll(SlruCtl ctl, bool allow_redirtied)
```
## Detailed Description
SimpleLruWriteAll is a critical function that performs bulk write operations of all dirty pages in an SLRU buffer pool. It is typically called during checkpoint operations or database shutdown to ensure data durability. The function iterates through all buffer slots, acquires appropriate bank locks, and writes dirty pages using SlruInternalWritePage. It handles file management by tracking opened files during the write process and properly closing them afterward. The function also includes error handling for file operations and ensures directory synchronization for newly created files.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing configuration, callback functions, and shared state
- `allow_redirtied`: Boolean flag indicating whether to allow pages to be re-dirtied during the write process (typically true for checkpoints, false for shutdown)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_slru_flush](../p/pgstat_count_slru_flush.md)
  - SlotGetBankNumber
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [SlruInternalWritePage](SlruInternalWritePage.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - [SlruReportIOError](SlruReportIOError.md)
  - [fsync_fname](../f/fsync_fname.md)
- Constants used:
  - SLRU_PAGE_EMPTY
  - SLRU_PAGE_VALID
  - SLRU_CLOSE_FAILED
  - SLRU_PAGES_PER_SEGMENT
  - SYNC_HANDLER_NONE
- Types used:
  - SlruCtl, SlruShared, SlruWriteAllData
- Called from:
  - [CheckPointCLOG](../C/CheckPointCLOG.md)
  - [CheckPointCommitTs](../C/CheckPointCommitTs.md)
  - [CheckPointMultiXact](../C/CheckPointMultiXact.md)
  - [CheckPointSUBTRANS](../C/CheckPointSUBTRANS.md)
  - [CheckPointPredicate](../C/CheckPointPredicate.md)
  - [find_multixact_start](../f/find_multixact_start.md)

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

## Simplified Source

```c
// Simplified version of SimpleLruWriteAll
void SimpleLruWriteAll(SlruCtl ctl, bool allow_redirtied) {
    SlruShared shared = ctl->shared;
    SlruWriteAllData file_data;
    int prev_bank = SlotGetBankNumber(0);
    bool write_ok = true;

    // Update flush statistics
    pgstat_count_slru_flush(shared->slru_stats_idx);

    // Initialize file tracking data
    file_data.num_files = 0;

    // Lock first bank and iterate through all buffer slots
    LWLockAcquire(&shared->bank_locks[prev_bank].lock, LW_EXCLUSIVE);

    for (int slot = 0; slot < shared->num_slots; slot++) {
        int current_bank = SlotGetBankNumber(slot);

        // Switch bank locks when moving to different bank
        if (current_bank != prev_bank) {
            LWLockRelease(&shared->bank_locks[prev_bank].lock);
            LWLockAcquire(&shared->bank_locks[current_bank].lock, LW_EXCLUSIVE);
            prev_bank = current_bank;
        }

        // Skip empty slots
        if (shared->page_status[slot] == SLRU_PAGE_EMPTY)
            continue;

        // Write the dirty page to disk
        SlruInternalWritePage(ctl, slot, &file_data);

        // Assert page state is valid (allowing for concurrent re-dirtying)
        Assert(allow_redirtied ||
               shared->page_status[slot] == SLRU_PAGE_EMPTY ||
               (shared->page_status[slot] == SLRU_PAGE_VALID &&
                !shared->page_dirty[slot]));
    }

    LWLockRelease(&shared->bank_locks[prev_bank].lock);

    // Close all opened files and handle errors
    for (int i = 0; i < file_data.num_files; i++) {
        if (CloseTransientFile(file_data.fd[i]) != 0) {
            // Set error information for reporting
            slru_errcause = SLRU_CLOSE_FAILED;
            slru_errno = errno;
            write_ok = false;
        }
    }

    // Report any file close errors
    if (!write_ok)
        SlruReportIOError(ctl, file_data.segno[0] * SLRU_PAGES_PER_SEGMENT,
                         InvalidTransactionId);

    // Sync directory to ensure new file entries are on disk
    if (ctl->sync_handler != SYNC_HANDLER_NONE)
        fsync_fname(ctl->Dir, true);
}
```

Key simplifications made:
- Consolidated variable declarations and simplified variable names
- Removed detailed comments, keeping only essential logic explanations
- Simplified error handling logic while preserving error reporting
- Clarified the bank locking strategy with clearer comments
- Abstracted the pageno calculation in error reporting
- Maintained the essential algorithm flow: iterate slots → write dirty pages → close files → sync directory
- Preserved all critical functionality including statistics, locking, writing, and error handling