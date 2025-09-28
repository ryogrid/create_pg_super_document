# SlruSelectLRUPage

## Location
[src/backend/access/transam/slru.c:1166-1318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1166-L1318)

## Overview
Selects the optimal slot to reuse when a free slot is needed for a given page, implementing the LRU (Least Recently Used) eviction policy with sophisticated tie-breaking and I/O optimization.

## Definition

```c
static int
SlruSelectLRUPage(SlruCtl ctl, int64 pageno)
```
## Detailed Description
SlruSelectLRUPage is a critical function that implements the page replacement algorithm for SLRU buffers. It first checks if the target page is already in a buffer slot, and if not, selects the least recently used page for eviction. The algorithm prioritizes empty slots, then clean valid pages, and finally dirty pages that require write-out. It includes optimizations to avoid evicting the most recently zeroed page, handles concurrent access scenarios, and deals with I/O-busy slots intelligently. The function operates within a bank-based architecture where pages are distributed across multiple banks for better concurrency.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing configuration and callback functions
- `pageno`: The page number for which a buffer slot is needed

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md)
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)  
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [SimpleLruWaitIO](SimpleLruWaitIO.md)
  - [SlruInternalWritePage](SlruInternalWritePage.md)
- Constants used:
  - SLRU_BANK_SIZE
  - SLRU_PAGE_EMPTY
  - SLRU_PAGE_VALID
- Types used:
  - SlruCtl
  - SlruShared
- Called from:
  - [SimpleLruZeroPage](SimpleLruZeroPage.md)
  - [SimpleLruReadPage](SimpleLruReadPage.md)

## Notes and Other Information
- Requires the appropriate bank lock to be held at entry and maintains it at exit
- Uses a sophisticated LRU algorithm that considers page access recency, I/O status, and page cleanliness
- Handles concurrent execution scenarios where multiple processes may update LRU counters simultaneously
- Implements tie-breaking by choosing the furthest-back page when LRU counts are equal
- Never selects the latest_page_number for eviction to protect recently zeroed pages
- Prefers to wait for I/O completion rather than selecting I/O-busy slots when possible
- Automatically handles dirty page write-out when necessary
- Uses atomic operations to read the latest page number for thread safety
- The outer loop structure allows for restart after I/O operations complete
- Implements counter wrap-around protection by adjusting page LRU counts when negative deltas are detected

## Simplified Source

```c
// Simplified version of SlruSelectLRUPage
static int SlruSelectLRUPage(SlruCtl ctl, int64 pageno) {
    SlruShared shared = ctl->shared;

    // Main loop: restart after I/O operations
    for (;;) {
        int cur_count;
        int bestvalidslot = 0, best_valid_delta = -1;
        int64 best_valid_page_number = 0;
        int bestinvalidslot = 0, best_invalid_delta = -1;
        int64 best_invalid_page_number = 0;

        // Calculate bank boundaries
        int bankno = pageno % ctl->nbanks;
        int bankstart = bankno * SLRU_BANK_SIZE;
        int bankend = bankstart + SLRU_BANK_SIZE;

        // Step 1: Check if target page already exists
        for (int slotno = bankstart; slotno < bankend; slotno++) {
            if (shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
                shared->page_number[slotno] == pageno)
                return slotno;
        }

        // Step 2: Find LRU victim page
        cur_count = (shared->bank_cur_lru_count[bankno])++;

        for (int slotno = bankstart; slotno < bankend; slotno++) {
            // Empty slot found - use it immediately
            if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
                return slotno;

            // Calculate LRU delta
            int this_delta = cur_count - shared->page_lru_count[slotno];
            if (this_delta < 0) {
                shared->page_lru_count[slotno] = cur_count;
                this_delta = 0;
            }

            int64 this_page_number = shared->page_number[slotno];

            // Skip the most recently zeroed page
            if (this_page_number == pg_atomic_read_u64(&shared->latest_page_number))
                continue;

            // Track best candidate by page status
            if (shared->page_status[slotno] == SLRU_PAGE_VALID) {
                if (this_delta > best_valid_delta ||
                    (this_delta == best_valid_delta &&
                     ctl->PagePrecedes(this_page_number, best_valid_page_number))) {
                    bestvalidslot = slotno;
                    best_valid_delta = this_delta;
                    best_valid_page_number = this_page_number;
                }
            } else {
                if (this_delta > best_invalid_delta ||
                    (this_delta == best_invalid_delta &&
                     ctl->PagePrecedes(this_page_number, best_invalid_page_number))) {
                    bestinvalidslot = slotno;
                    best_invalid_delta = this_delta;
                    best_invalid_page_number = this_page_number;
                }
            }
        }

        // Step 3: Handle selection result
        if (best_valid_delta < 0) {
            // All pages are I/O busy - wait for completion
            SimpleLruWaitIO(ctl, bestinvalidslot);
            continue;
        }

        // Clean page found - use it
        if (!shared->page_dirty[bestvalidslot])
            return bestvalidslot;

        // Dirty page - write it out and retry
        SlruInternalWritePage(ctl, bestvalidslot, NULL);
    }
}
```

Key simplifications made:
- Removed detailed algorithmic comments while preserving step-by-step logic
- Consolidated variable declarations for clarity
- Simplified LRU selection logic explanation
- Focused on the three main steps: page existence check, LRU victim selection, and result handling
- Preserved all essential tie-breaking and I/O optimization logic
- Maintained the infinite loop structure for I/O retry scenarios