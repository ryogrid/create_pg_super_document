# SimpleLruShmemSize

## Location
[src/backend/access/transam/slru.c:199-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L199-L231)

## Overview
SimpleLruShmemSize calculates the total shared memory size required for a Simple LRU buffer management structure, including all associated data structures and page buffers.

## Definition


## Detailed Description
This function computes the total amount of shared memory needed to initialize an SLRU (Simple LRU) control structure with the specified number of buffer slots and LSN groups. The calculation includes space for:

1. **Core shared data structure** (SlruSharedData)
2. **Buffer management arrays** for each slot:
   - page_buffer[] - pointers to page buffers
   - page_status[] - status of each page  
   - page_dirty[] - dirty flags
   - page_number[] - logical page numbers
   - page_lru_count[] - LRU counters
3. **Locking structures**:
   - buffer_locks[] - per-buffer locks
   - bank_locks[] - per-bank locks for concurrent access
   - bank_cur_lru_count[] - per-bank LRU counters
4. **LSN tracking** (if nlsns > 0):
   - group_lsn[] - LSN values for WAL consistency
5. **Actual page buffers** - BLCKSZ bytes per slot

The function organizes buffers into banks (groups of SLRU_BANK_SIZE buffers) to improve concurrency by reducing lock contention. All memory allocations are properly aligned using MAXALIGN and BUFFERALIGN macros.

## Parameters / Member Variables
- : Number of buffer slots to allocate. Must be divisible by SLRU_BANK_SIZE and not exceed SLRU_MAX_ALLOWED_BUFFERS
- : Number of LSN groups for WAL consistency tracking. If 0, no LSN tracking space is allocated

## Dependencies
- Functions called/Symbols referenced:
  - SLRU_BANK_SIZE (constant defining buffers per bank)
  - SLRU_MAX_ALLOWED_BUFFERS (maximum allowed buffer slots)
  - [SlruSharedData](SlruSharedData.md) (main shared data structure type)
  - SlruPageStatus (enum for page status values)
  - LWLockPadded (padded lightweight lock structure)
  - MAXALIGN (memory alignment macro)
  - BUFFERALIGN (buffer alignment macro)
  - BLCKSZ (block size constant)

- Called from (representative examples):
  - CLOGShmemSize
  - CommitTsShmemSize
  - SUBTRANSShmemSize
  - [SimpleLruInit](SimpleLruInit.md)
  - [PredicateLockShmemSize](../P/PredicateLockShmemSize.md)
  - [AsyncShmemSize](../A/AsyncShmemSize.md)

## Notes and Other Information
- The function includes assertions to validate that nslots is within allowed bounds and properly aligned to bank boundaries
- The banking system (nbanks = nslots / SLRU_BANK_SIZE) is designed to reduce lock contention in high-concurrency scenarios
- Memory size calculation assumes nslots won't cause integer overflow
- The final buffer alignment ensures proper memory layout for the actual page data
- This function is typically called during shared memory initialization to determine memory requirements before allocation
- Different PostgreSQL subsystems (CLOG, CommitTS, SUBTRANS, etc.) use this to size their SLRU buffers appropriately