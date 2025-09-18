# pgstat_end_changecount_write

## Location
src/include/utils/pgstat_internal.h: 722 - 733

## Overview
This inline function completes a write operation to change-counted statistics by incrementing the change counter again and releasing the critical section, signaling that the write operation has finished.

## Definition
```c
static inline void
pgstat_end_changecount_write(uint32 *cc)
```

## Detailed Description
This function implements the completion phase of PostgreSQL's change-count protocol for protecting shared memory statistics. It must be called after pgstat_begin_changecount_write to properly conclude a write operation sequence.

When called, the function:
1. Asserts that the change counter is odd (write operation was in progress)
2. Issues a write memory barrier to ensure all writes are completed before counter update
3. Increments the change counter (making it even to indicate write completion)
4. Ends the critical section to allow interruptions again

The dual increment approach (begin: even→odd, end: odd→even) allows concurrent readers to detect both ongoing writes and completed write sequences.

## Parameters / Member Variables
- `cc`: Pointer to a 32-bit change counter that tracks the state of write operations (even = no write, odd = write in progress)

## Dependencies
- Functions called/Symbols referenced:
  - pg_write_barrier (memory barrier function)
  - END_CRIT_SECTION (macro to end critical section)
- Called from (representative examples):
  - [pgstat_report_archiver](pgstat_report_archiver.md)

## Notes and Other Information
- This function must always be paired with a preceding pgstat_begin_changecount_write call
- The write barrier ensures that all data writes are completed before the counter is incremented to signal completion
- The Assert verifies that the protocol is being followed correctly (counter should be odd before write ends)
- After this call, the change counter will be even, allowing readers to proceed safely
- The critical section prevents process interruption during the entire write sequence
- This completes the lock-free synchronization mechanism that allows statistics collection without blocking