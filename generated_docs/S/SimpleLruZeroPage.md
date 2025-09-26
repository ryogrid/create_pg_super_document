# SimpleLruZeroPage

## Location
[src/backend/access/transam/slru.c:375-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L375-L427)

## Overview
Initializes or reinitializes a SLRU page to zeroes in shared memory without writing it to disk, returning the slot number of the new page.

## Definition

```c
int
SimpleLruZeroPage(SlruCtl ctl, int64 pageno)
```
## Detailed Description
SimpleLruZeroPage creates a new SLRU (Simple Least Recently Used) page filled with zeros in the shared buffer pool. This function is used when a new page needs to be allocated in various PostgreSQL subsystems like CLOG, commit timestamp, multixact, and subtrans systems. The page is marked as valid and dirty in memory but is not immediately written to disk. The function ensures proper synchronization by requiring the caller to hold the appropriate bank lock in exclusive mode.

The function performs several key operations:
1. Selects an appropriate buffer slot using LRU policy
2. Marks the slot as containing the specified page number
3. Sets the page status to valid and dirty
4. Zeros out the entire page buffer
5. Initializes LSN values for the page
6. Updates the latest page number atomically
7. Increments statistics for zeroed pages

## Parameters / Member Variables
- : SlruCtl structure containing the SLRU control information and shared state
- : The 64-bit page number to be zeroed and initialized

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md) (assertion check for lock held)
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md) (bank lock acquisition)
  - [SlruSelectLRUPage](SlruSelectLRUPage.md) (LRU page selection)
  - [SlruRecentlyUsed](SlruRecentlyUsed.md) (mark page as recently used)
  - MemSet (zero the page buffer)
  - [SimpleLruZeroLSNs](SimpleLruZeroLSNs.md) (zero LSN values)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md) (atomic update of latest page number)
  - [pgstat_count_slru_page_zeroed](../p/pgstat_count_slru_page_zeroed.md) (statistics update)
- Called from (representative examples):
  - [ZeroCLOGPage](../Z/ZeroCLOGPage.md)
  - [ZeroCommitTsPage](../Z/ZeroCommitTsPage.md)
  - [ZeroMultiXactOffsetPage](../Z/ZeroMultiXactOffsetPage.md)
  - [ZeroMultiXactMemberPage](../Z/ZeroMultiXactMemberPage.md)
  - [ZeroSUBTRANSPage](../Z/ZeroSUBTRANSPage.md)
  - [asyncQueueAddEntries](../a/asyncQueueAddEntries.md)
  - [SerialAdd](SerialAdd.md)

## Notes and Other Information
- The bank lock must be held in exclusive mode before calling this function
- The page is marked as dirty but not immediately flushed to disk
- The function atomically updates the latest page number to maintain consistency
- Memory barriers are not required due to the ControlLock being held during execution
- The function includes assertions to verify proper page states and lock holding
- Statistics are updated to track the number of pages that have been zeroed