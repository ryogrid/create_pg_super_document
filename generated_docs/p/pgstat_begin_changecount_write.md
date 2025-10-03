# pgstat_begin_changecount_write

## Location
[src/include/utils/pgstat_internal.h:712-721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L712-L721)

## Overview
This inline function initiates a write operation to change-counted statistics by incrementing a change counter and establishing memory barriers to ensure data consistency during concurrent access.

## Definition

```c
static inline void
pgstat_begin_changecount_write(uint32 *cc)
```
## Detailed Description
This function implements the beginning phase of PostgreSQL's change-count protocol, which is used to protect shared memory statistics from corruption during concurrent reads and writes. The change-count protocol ensures that readers can detect when a write is in progress and retry their reads accordingly.

When called, the function:
1. Asserts that the change counter is even (no write currently in progress)
2. Starts a critical section to prevent interruptions
3. Increments the change counter (making it odd to indicate write in progress)
4. Issues a write memory barrier to ensure ordering

This follows a standard reader-writer synchronization pattern where odd counter values indicate write operations are active.

## Parameters / Member Variables
- `*cc`: Pointer to a 32-bit change counter that tracks the state of write operations (even = no write, odd = write in progress)
## Dependencies
- Functions called/Symbols referenced:
  - START_CRIT_SECTION (macro to begin critical section)
  - pg_write_barrier (memory barrier function)
- Called from (representative examples):
  - [pgstat_report_archiver](pgstat_report_archiver.md)
  - [pgstat_report_bgwriter](pgstat_report_bgwriter.md)
  - [pgstat_report_checkpointer](pgstat_report_checkpointer.md)

## Notes and Other Information
- This function must always be paired with pgstat_end_changecount_write
- The critical section prevents the process from being interrupted during the write sequence
- The write barrier ensures that the counter increment is visible before any subsequent writes to the protected data
- The Assert ensures the protocol is being followed correctly (counter should be even before write begins)
- This is part of PostgreSQL's lock-free statistics collection system that allows reading statistics without blocking writers