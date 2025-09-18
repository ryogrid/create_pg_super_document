# pgstat_begin_changecount_read

## Location
[src/include/utils/pgstat_internal.h:734-748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L734-L748)

## Overview
This inline function initiates a read operation from change-counted statistics by capturing the current change counter value and establishing memory barriers to ensure consistent reads during concurrent access.

## Definition
```c
static inline uint32
pgstat_begin_changecount_read(uint32 *cc)
```

## Detailed Description
This function implements the beginning phase of the reader side of PostgreSQL's change-count protocol. It captures the current state of the change counter before reading shared statistics data, allowing the reader to later detect if any writes occurred during the read operation.

When called, the function:
1. Captures the current change counter value before reading
2. Checks for interrupts to handle any pending signals
3. Issues a read memory barrier to ensure proper memory ordering
4. Returns the captured counter value for later verification

The returned value is used with pgstat_end_changecount_read to determine if the read operation was consistent (no concurrent writes).

## Parameters / Member Variables
- `cc`: Pointer to a 32-bit change counter that tracks the state of write operations

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (macro to handle pending interrupts)
  - pg_read_barrier (memory barrier function)
- Called from (representative examples):
  - [pgstat_copy_changecounted_stats](pgstat_copy_changecounted_stats.md)

## Notes and Other Information
- This function must always be paired with pgstat_end_changecount_read
- The returned value represents the change counter state before the read began
- The read barrier ensures that the counter read happens before any subsequent data reads
- CHECK_FOR_INTERRUPTS allows the process to handle signals before starting the read
- If the counter is odd when this function reads it, it indicates a write is in progress
- This is part of a lock-free protocol that allows reading statistics without blocking writers