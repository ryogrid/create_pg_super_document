# SlruSharedData

## Location
[src/include/access/slru.h:61-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/slru.h#L61-L119)

## Overview
SlruSharedData is a shared-memory structure that maintains the complete state information for PostgreSQL's Simple LRU (SLRU) buffer management system, including buffer metadata, locking mechanisms, and statistical tracking.

## Definition

```c
typedef struct SlruSharedData
{
	/* Number of buffers managed by this SLRU structure */
	int			num_slots;

	/*
	 * Arrays holding info for each buffer slot.  Page number is undefined
	 * when status is EMPTY, as is page_lru_count.
	 */
	char	  **page_buffer;
	SlruPageStatus *page_status;
	bool	   *page_dirty;
	int64	   *page_number;
	int		   *page_lru_count;

	/* The buffer_locks protects the I/O on each buffer slots */
	LWLockPadded *buffer_locks;

	/* Locks to protect the in memory buffer slot access in SLRU bank. */
	LWLockPadded *bank_locks;

	/*----------
	 * A bank-wise LRU counter is maintained because we do a victim buffer
	 * search within a bank. Furthermore, manipulating an individual bank
	 * counter avoids frequent cache invalidation since we update it every time
	 * we access the page.
	 *
	 * We mark a page "most recently used" by setting
	 *		page_lru_count[slotno] = ++bank_cur_lru_count[bankno];
	 * The oldest page in the bank is therefore the one with the highest value
	 * of
	 * 		bank_cur_lru_count[bankno] - page_lru_count[slotno]
	 * The counts will eventually wrap around, but this calculation still
	 * works as long as no page's age exceeds INT_MAX counts.
	 *----------
	 */
	int		   *bank_cur_lru_count;

	/*
	 * Optional array of WAL flush LSNs associated with entries in the SLRU
	 * pages.  If not zero/NULL, we must flush WAL before writing pages (true
	 * for pg_xact, false for everything else).  group_lsn[] has
	 * lsn_groups_per_page entries per buffer slot, each containing the
	 * highest LSN known for a contiguous group of SLRU entries on that slot's
	 * page.
	 */
	XLogRecPtr *group_lsn;
	int			lsn_groups_per_page;

	/*
	 * latest_page_number is the page number of the current end of the log;
	 * this is not critical data, since we use it only to avoid swapping out
	 * the latest page.
	 */
	pg_atomic_uint64 latest_page_number;

	/* SLRU's index for statistics purposes (might not be unique) */
	int			slru_stats_idx;
} SlruSharedData;
```
## Detailed Description
SlruSharedData serves as the central control structure for SLRU buffer pools in PostgreSQL's shared memory. It manages arrays of buffer slots, each containing metadata about cached pages, along with sophisticated locking and LRU tracking mechanisms. The structure implements a bank-based architecture where buffers are divided into banks, each protected by separate locks to reduce contention.

The LRU (Least Recently Used) replacement policy is implemented using bank-specific counters that track page access patterns. The structure also supports WAL (Write-Ahead Logging) integration through LSN tracking for certain SLRU types like transaction status (pg_xact).

## Parameters / Member Variables
- `num_slots`: int - Total number of buffer slots managed by this SLRU instance
- `**page_buffer`: char** - Array of pointers to actual page data buffers
- `*page_status`: SlruPageStatus* - Array tracking the status of each buffer slot (EMPTY, READ_IN_PROGRESS, VALID, WRITE_IN_PROGRESS)
- `*page_dirty`: bool* - Array indicating whether each page has been modified and needs writing
- `*page_number`: int64* - Array storing the logical page number for each buffer slot
- `*page_lru_count`: int* - Array of LRU counters for each page, used in replacement decisions
- `*buffer_locks`: LWLockPadded* - Array of locks protecting I/O operations on individual buffer slots
- `*bank_locks`: LWLockPadded* - Array of locks protecting in-memory access to buffer slots within each bank
- `*bank_cur_lru_count`: int* - Array of current LRU counters per bank for efficient victim page selection
- `*group_lsn`: XLogRecPtr* - Optional array of WAL LSNs for groups of entries (used by pg_xact)
- `lsn_groups_per_page`: int - Number of LSN groups tracked per page when group_lsn is used
- `latest_page_number`: pg_atomic_uint64 - Atomic counter tracking the current end of the log to avoid swapping out active pages
- `slru_stats_idx`: int - Index used for statistics collection and reporting
## Dependencies
- Functions called/Symbols referenced:
  - SlruPageStatus (enumeration for page states)
  - LWLockPadded (padded lightweight locks)
  - XLogRecPtr (WAL log sequence number type)
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md) (atomic 64-bit integer)

- Called from (representative examples):
  - [SimpleLruShmemSize](SimpleLruShmemSize.md) (slru.c:208)
  - [SimpleLruInit](SimpleLruInit.md) (slru.c:274, 284)
  - SlruShared (slru.h:121)

## Notes and Other Information
- All fields except latest_page_number are protected by ControlLock; latest_page_number uses atomic operations
- The bank-based LRU algorithm reduces cache invalidation by maintaining separate counters per bank
- LRU counting uses wraparound-safe arithmetic: 
- WAL integration through group_lsn is optional and primarily used by transaction status SLRU (pg_xact)
- The structure is allocated in shared memory and accessed by multiple PostgreSQL backend processes
- Buffer slots can be in one of four states: EMPTY, READ_IN_PROGRESS, VALID, or WRITE_IN_PROGRESS
- Statistical tracking through slru_stats_idx enables monitoring of SLRU performance across different subsystems