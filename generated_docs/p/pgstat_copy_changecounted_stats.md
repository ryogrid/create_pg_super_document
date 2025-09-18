# pgstat_copy_changecounted_stats

## Location
src/include/utils/pgstat_internal.h: 773 - 788

## Overview
This inline helper function safely copies statistics data from shared memory using the change-count protocol, automatically retrying the copy operation until a consistent read is achieved.

## Definition
```c
static inline void
pgstat_copy_changecounted_stats(void *dst, void *src, size_t len, uint32 *cc)
```

## Detailed Description
This function implements a complete change-counted read operation that safely copies statistics data from shared memory to a destination buffer. It encapsulates the full reader protocol, automatically handling retries when concurrent writes are detected.

The function operates in a retry loop that:
1. Captures the initial change counter state
2. Performs the memory copy operation
3. Validates that no writes occurred during the copy
4. Repeats the entire process if the read was inconsistent

This ensures that the copied data represents a consistent snapshot of the statistics, never containing partially updated values from concurrent writers.

## Parameters / Member Variables
- `dst`: Destination buffer where statistics data will be copied
- `src`: Source location of the statistics data in shared memory  
- `len`: Number of bytes to copy
- `cc`: Pointer to the change counter protecting the source data

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_begin_changecount_read](pgstat_begin_changecount_read.md) (initiate read sequence)
  - [pgstat_end_changecount_read](pgstat_end_changecount_read.md) (validate read consistency)
  - memcpy (perform the actual data copy)
- Called from (representative examples):
  - [pgstat_archiver_reset_all_cb](pgstat_archiver_reset_all_cb.md)
  - [pgstat_archiver_snapshot_cb](pgstat_archiver_snapshot_cb.md)
  - [pgstat_bgwriter_reset_all_cb](pgstat_bgwriter_reset_all_cb.md)
  - [pgstat_bgwriter_snapshot_cb](pgstat_bgwriter_snapshot_cb.md)
  - [pgstat_checkpointer_reset_all_cb](pgstat_checkpointer_reset_all_cb.md)
  - [pgstat_checkpointer_snapshot_cb](pgstat_checkpointer_snapshot_cb.md)

## Notes and Other Information
- This is a higher-level convenience function that implements the complete reader-side protocol
- Automatically handles retry logic, so callers don't need to implement their own retry loops
- Used primarily in snapshot and reset callbacks for various PostgreSQL background processes
- The retry loop will continue until a consistent read is achieved, which could potentially be indefinite if writes are very frequent
- This function provides the standard way to safely read change-counted statistics data in PostgreSQL
- Essential for maintaining data consistency in PostgreSQL's lock-free statistics collection system