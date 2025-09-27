# SimpleLruTruncate

## Location
[src/backend/access/transam/slru.c:1405-1499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1405-L1499)

## Overview
Removes all segments before the one holding the specified cutoff page number, safely truncating old SLRU data while handling concurrent access and I/O operations.

## Definition

```c
void
SimpleLruTruncate(SlruCtl ctl, int64 cutoffPage)
```
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
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
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

## Simplified Source

```c
// Simplified version of SimpleLruTruncate
void SimpleLruTruncate(SlruCtl ctl, int64 cutoffPage) {
    SlruShared shared = ctl->shared;
    int prevbank;

    // Update truncation statistics
    pgstat_count_slru_truncate(shared->slru_stats_idx);

    // Safety check: prevent wraparound bugs
    if (ctl->PagePrecedes(pg_atomic_read_u64(&shared->latest_page_number),
                          cutoffPage)) {
        ereport(LOG, (errmsg("could not truncate directory \"%s\": apparent wraparound",
                             ctl->Dir)));
        return;
    }

    // Clean up memory buffers for pages before cutoff
restart:
    prevbank = SlotGetBankNumber(0);
    LWLockAcquire(&shared->bank_locks[prevbank].lock, LW_EXCLUSIVE);

    for (int slotno = 0; slotno < shared->num_slots; slotno++) {
        int curbank = SlotGetBankNumber(slotno);

        // Switch bank locks as needed
        if (curbank != prevbank) {
            LWLockRelease(&shared->bank_locks[prevbank].lock);
            LWLockAcquire(&shared->bank_locks[curbank].lock, LW_EXCLUSIVE);
            prevbank = curbank;
        }

        // Skip empty slots or pages we want to keep
        if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
            continue;
        if (!ctl->PagePrecedes(shared->page_number[slotno], cutoffPage))
            continue;

        // Handle clean pages - just mark as empty
        if (shared->page_status[slotno] == SLRU_PAGE_VALID &&
            !shared->page_dirty[slotno]) {
            shared->page_status[slotno] = SLRU_PAGE_EMPTY;
            continue;
        }

        // Handle I/O-busy pages - write or wait, then restart
        if (shared->page_status[slotno] == SLRU_PAGE_VALID)
            SlruInternalWritePage(ctl, slotno, NULL);
        else
            SimpleLruWaitIO(ctl, slotno);

        LWLockRelease(&shared->bank_locks[prevbank].lock);
        goto restart;
    }

    LWLockRelease(&shared->bank_locks[prevbank].lock);

    // Remove old segment files from disk
    (void) SlruScanDirectory(ctl, SlruScanDirCbDeleteCutoff, &cutoffPage);
}
```

Key simplifications made:
- Preserved the essential two-phase approach: memory cleanup → disk cleanup
- Maintained critical safety checks and restart logic for I/O handling
- Simplified bank locking strategy while preserving correctness
- Focused on the core algorithm: check safety → clean memory → remove files
- Removed detailed debug instrumentation while keeping essential error reporting