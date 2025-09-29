# SimpleLruReadPage

## Location
[src/backend/access/transam/slru.c:502-604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L502-L604)

## Overview
Finds a page in the SLRU shared buffer pool, reading it from disk if necessary, with support for concurrent I/O operations and transaction safety.

## Definition

```c
int
SimpleLruReadPage(SlruCtl ctl, int64 pageno, bool write_ok,
				  TransactionId xid)
```
## Detailed Description
SimpleLruReadPage is the core function for accessing SLRU pages in PostgreSQL's buffer management system. It implements a sophisticated page lookup and loading mechanism that handles concurrent access, I/O operations, and error conditions. The function first searches for the requested page in the shared buffer pool, and if not found, selects a victim page using LRU policy and reads the required page from disk.

The function operates in a loop to handle cases where concurrent I/O operations require waiting. It supports both read-only and write-enabled access modes through the write_ok parameter. When a page is being written and write_ok is false, the function will wait for the write to complete. The function also maintains proper locking protocols and updates LRU information and statistics.

Key operations performed:
1. Check if the page is already in memory using SlruSelectLRUPage
2. Handle concurrent I/O operations by waiting if necessary  
3. If page not found, mark a slot as read-in-progress
4. Read the page from disk using SlruPhysicalReadPage
5. Initialize LSN values and update page status
6. Update LRU information and statistics

## Parameters / Member Variables
- : SlruCtl structure containing the SLRU control information and shared state
- : The 64-bit page number to read
- : Boolean indicating whether it's acceptable to return a page that's being written
- : Transaction ID used for error reporting (may be InvalidTransactionId)

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md) (get bank lock for page)
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md) (assert lock is held)
  - [SlruSelectLRUPage](SlruSelectLRUPage.md) (find page or select victim slot)
  - [SimpleLruWaitIO](SimpleLruWaitIO.md) (wait for I/O completion)
  - [SlruRecentlyUsed](SlruRecentlyUsed.md) (update LRU information)
  - [SlruPhysicalReadPage](SlruPhysicalReadPage.md) (perform disk read)
  - [SimpleLruZeroLSNs](SimpleLruZeroLSNs.md) (initialize LSN values)
  - [SlruReportIOError](SlruReportIOError.md) (report I/O errors)
  - [pgstat_count_slru_page_hit](../p/pgstat_count_slru_page_hit.md)/read (update statistics)
- Called from (representative examples):
  - [TransactionIdSetPageStatusInternal](../T/TransactionIdSetPageStatusInternal.md)
  - [SetXidCommitTsInPage](SetXidCommitTsInPage.md)
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [SubTransSetParent](SubTransSetParent.md)
  - [asyncQueueAddEntries](../a/asyncQueueAddEntries.md)
  - [SerialAdd](SerialAdd.md)
  - [SimpleLruReadPage_ReadOnly](SimpleLruReadPage_ReadOnly.md)

## Notes and Other Information
- The correct bank lock must be held in exclusive mode at entry and exit
- The function handles concurrent read and write operations safely
- Uses a retry loop to handle cases where I/O operations require waiting
- Updates both LRU access information and statistics counters
- Supports transaction-aware error reporting through the xid parameter
- The write_ok parameter allows callers to specify whether concurrent writes are acceptable
- Proper lock ordering prevents deadlocks during I/O operations
- LSN values are zeroed for newly read pages to ensure WAL consistency
- Statistics distinguish between cache hits and disk reads for performance monitoring

## Simplified Source

```c
// Simplified version of SimpleLruReadPage
int SimpleLruReadPage(SlruCtl ctl, int64 pageno, bool write_ok, TransactionId xid) {
    SlruShared shared = ctl->shared;
    LWLock *banklock = SimpleLruGetBankLock(ctl, pageno);

    // Main loop: retry if we need to wait for I/O
    for (;;) {
        int slotno;
        bool read_ok;

        // Step 1: Find page in memory or select victim slot
        slotno = SlruSelectLRUPage(ctl, pageno);

        // Step 2: Check if page is already in memory
        if (shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
            shared->page_number[slotno] == pageno) {

            // Handle concurrent I/O - wait if page is being read/written
            if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
                (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS && !write_ok)) {
                SimpleLruWaitIO(ctl, slotno);
                continue;  // Retry from beginning
            }

            // Page is ready - update LRU info and return
            SlruRecentlyUsed(shared, slotno);
            pgstat_count_slru_page_hit(shared->slru_stats_idx);
            return slotno;
        }

        // Step 3: Page not in memory - prepare for disk read
        shared->page_number[slotno] = pageno;
        shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
        shared->page_dirty[slotno] = false;

        // Step 4: Acquire buffer lock and release bank lock for I/O
        LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);
        LWLockRelease(banklock);

        // Step 5: Read page from disk
        read_ok = SlruPhysicalReadPage(ctl, pageno, slotno);
        SimpleLruZeroLSNs(ctl, slotno);

        // Step 6: Re-acquire bank lock and update page status
        LWLockAcquire(banklock, LW_EXCLUSIVE);
        shared->page_status[slotno] = read_ok ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY;
        LWLockRelease(&shared->buffer_locks[slotno].lock);

        // Step 7: Handle read errors
        if (!read_ok) {
            SlruReportIOError(ctl, pageno, xid);
        }

        // Step 8: Update LRU info and stats, then return
        SlruRecentlyUsed(shared, slotno);
        pgstat_count_slru_page_read(shared->slru_stats_idx);
        return slotno;
    }
}
```

Key simplifications made:
- Removed detailed assertions for clarity while keeping essential logic flow
- Simplified complex nested conditions into clearer step-by-step operations
- Added descriptive comments explaining each major phase
- Consolidated error handling into a single location
- Used more descriptive variable names (read_ok instead of ok)
- Structured the code to show the main algorithm phases clearly
- Removed low-level memory operation details while preserving core functionality