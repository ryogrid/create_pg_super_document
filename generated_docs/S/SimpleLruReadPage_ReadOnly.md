# SimpleLruReadPage_ReadOnly

## Location
[src/backend/access/transam/slru.c:605-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L605-L651)

## Overview
Optimized function for read-only access to SLRU pages that first attempts to find the page using only shared locks before falling back to exclusive lock access.

## Definition


## Detailed Description
SimpleLruReadPage_ReadOnly is an optimized variant of SimpleLruReadPage designed specifically for read-only access patterns. The function implements a two-phase approach: first attempting to locate the requested page while holding only a shared bank lock to minimize contention, and if that fails, falling back to the standard exclusive lock approach used by SimpleLruReadPage.

The optimization is based on the observation that many SLRU accesses are read-only and can be satisfied without exclusive locking if the page is already in memory. This reduces lock contention and improves concurrency for read-heavy workloads.

The function operates by:
1. Acquiring the bank lock in shared mode
2. Scanning through the bank's buffer slots to find the requested page
3. If found and not being read, returning the slot immediately
4. If not found, upgrading to exclusive lock and calling SimpleLruReadPage

This approach provides better performance for read-only access patterns while maintaining full consistency and safety guarantees.

## Parameters / Member Variables
- : SlruCtl structure containing the SLRU control information and shared state
- : The 64-bit page number to read
- : Transaction ID used for error reporting (may be InvalidTransactionId)

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md) (get bank lock for page)
  - LWLockAcquire (acquire locks in shared/exclusive modes)
  - LWLockRelease (release bank lock for upgrade)
  - [SlruRecentlyUsed](SlruRecentlyUsed.md) (update LRU information)
  - [pgstat_count_slru_page_hit](../p/pgstat_count_slru_page_hit.md) (update statistics)
  - [SimpleLruReadPage](SimpleLruReadPage.md) (fallback for exclusive access)
- Called from (representative examples):
  - TransactionIdGetStatus
  - [TransactionIdGetCommitTsData](../T/TransactionIdGetCommitTsData.md)
  - [find_multixact_start](../f/find_multixact_start.md)
  - [SubTransGetParent](SubTransGetParent.md)
  - [asyncQueueReadAllNotifications](../a/asyncQueueReadAllNotifications.md)
  - [SerialGetMinConflictCommitSeqNo](SerialGetMinConflictCommitSeqNo.md)

## Notes and Other Information
- Optimized for read-only access patterns to reduce lock contention
- Bank control lock must NOT be held at entry but will be held at exit
- The lock mode at exit is unspecified (could be shared or exclusive)
- Uses shared lock first for better concurrency, then upgrades if necessary
- Skips pages that are currently being read (SLRU_PAGE_READ_IN_PROGRESS)
- Falls back to SimpleLruReadPage with write_ok=true for full functionality
- Scans only the relevant bank's slots rather than using the full LRU mechanism
- Updates LRU information and statistics when page is found with shared lock
- Provides the same safety guarantees as SimpleLruReadPage while improving performance