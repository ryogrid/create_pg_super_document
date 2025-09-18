# SimpleLruReadPage

## Location
src/backend/access/transam/slru.c: 502 - 604

## Overview
Finds a page in the SLRU shared buffer pool, reading it from disk if necessary, with support for concurrent I/O operations and transaction safety.

## Definition


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
  - SimpleLruGetBankLock (get bank lock for page)
  - LWLockHeldByMeInMode (assert lock is held)
  - SlruSelectLRUPage (find page or select victim slot)
  - SimpleLruWaitIO (wait for I/O completion)
  - SlruRecentlyUsed (update LRU information)
  - SlruPhysicalReadPage (perform disk read)
  - SimpleLruZeroLSNs (initialize LSN values)
  - SlruReportIOError (report I/O errors)
  - pgstat_count_slru_page_hit/read (update statistics)
- Called from (representative examples):
  - TransactionIdSetPageStatusInternal
  - SetXidCommitTsInPage
  - RecordNewMultiXact
  - GetMultiXactIdMembers
  - SubTransSetParent
  - asyncQueueAddEntries
  - SerialAdd
  - SimpleLruReadPage_ReadOnly

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