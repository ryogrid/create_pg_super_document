# SlruSharedData

## Location
src/include/access/slru.h: 61 - 119

## Overview
SlruSharedData is a shared-memory structure that maintains the complete state information for PostgreSQL's Simple LRU (SLRU) buffer management system, including buffer metadata, locking mechanisms, and statistical tracking.

## Definition


## Detailed Description
SlruSharedData serves as the central control structure for SLRU buffer pools in PostgreSQL's shared memory. It manages arrays of buffer slots, each containing metadata about cached pages, along with sophisticated locking and LRU tracking mechanisms. The structure implements a bank-based architecture where buffers are divided into banks, each protected by separate locks to reduce contention.

The LRU (Least Recently Used) replacement policy is implemented using bank-specific counters that track page access patterns. The structure also supports WAL (Write-Ahead Logging) integration through LSN tracking for certain SLRU types like transaction status (pg_xact).

## Parameters / Member Variables
- : int - Total number of buffer slots managed by this SLRU instance
- : char** - Array of pointers to actual page data buffers
- : SlruPageStatus* - Array tracking the status of each buffer slot (EMPTY, READ_IN_PROGRESS, VALID, WRITE_IN_PROGRESS)
- : bool* - Array indicating whether each page has been modified and needs writing
- : int64* - Array storing the logical page number for each buffer slot
- : int* - Array of LRU counters for each page, used in replacement decisions
- : LWLockPadded* - Array of locks protecting I/O operations on individual buffer slots
- : LWLockPadded* - Array of locks protecting in-memory access to buffer slots within each bank
- : int* - Array of current LRU counters per bank for efficient victim page selection
- : XLogRecPtr* - Optional array of WAL LSNs for groups of entries (used by pg_xact)
- : int - Number of LSN groups tracked per page when group_lsn is used
- : pg_atomic_uint64 - Atomic counter tracking the current end of the log to avoid swapping out active pages
- : int - Index used for statistics collection and reporting

## Dependencies
- Functions called/Symbols referenced:
  - SlruPageStatus (enumeration for page states)
  - LWLockPadded (padded lightweight locks)
  - XLogRecPtr (WAL log sequence number type)
  - pg_atomic_uint64 (atomic 64-bit integer)

- Called from (representative examples):
  - SimpleLruShmemSize (slru.c:208)
  - SimpleLruInit (slru.c:274, 284)
  - SlruShared (slru.h:121)

## Notes and Other Information
- All fields except latest_page_number are protected by ControlLock; latest_page_number uses atomic operations
- The bank-based LRU algorithm reduces cache invalidation by maintaining separate counters per bank
- LRU counting uses wraparound-safe arithmetic: 
- WAL integration through group_lsn is optional and primarily used by transaction status SLRU (pg_xact)
- The structure is allocated in shared memory and accessed by multiple PostgreSQL backend processes
- Buffer slots can be in one of four states: EMPTY, READ_IN_PROGRESS, VALID, or WRITE_IN_PROGRESS
- Statistical tracking through slru_stats_idx enables monitoring of SLRU performance across different subsystems