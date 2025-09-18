# SlruSelectLRUPage

## Location
src/backend/access/transam/slru.c: 1166 - 1318

## Overview
Selects the optimal slot to reuse when a free slot is needed for a given page, implementing the LRU (Least Recently Used) eviction policy with sophisticated tie-breaking and I/O optimization.

## Definition


## Detailed Description
SlruSelectLRUPage is a critical function that implements the page replacement algorithm for SLRU buffers. It first checks if the target page is already in a buffer slot, and if not, selects the least recently used page for eviction. The algorithm prioritizes empty slots, then clean valid pages, and finally dirty pages that require write-out. It includes optimizations to avoid evicting the most recently zeroed page, handles concurrent access scenarios, and deals with I/O-busy slots intelligently. The function operates within a bank-based architecture where pages are distributed across multiple banks for better concurrency.

## Parameters / Member Variables
- `ctl`: SLRU control structure containing configuration and callback functions
- `pageno`: The page number for which a buffer slot is needed

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruGetBankLock
  - LWLockHeldByMe  
  - pg_atomic_read_u64
  - SimpleLruWaitIO
  - SlruInternalWritePage
- Constants used:
  - SLRU_BANK_SIZE
  - SLRU_PAGE_EMPTY
  - SLRU_PAGE_VALID
- Types used:
  - SlruCtl
  - SlruShared
- Called from:
  - SimpleLruZeroPage
  - SimpleLruReadPage

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