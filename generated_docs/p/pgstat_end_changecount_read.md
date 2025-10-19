# pgstat_end_changecount_read

## Location
[src/include/utils/pgstat_internal.h:749-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L749-L772)

## Overview
This inline function completes a read operation from change-counted statistics by comparing the initial and final change counter values to determine if the read was consistent (no concurrent writes occurred).

## Definition
```c
static inline bool
pgstat_end_changecount_read(uint32 *cc, uint32 before_cc)
```

## Detailed Description
This function implements the completion phase of the reader side of PostgreSQL's change-count protocol. It validates whether a read operation was performed consistently by checking if any write operations occurred during the read sequence.

The function performs two critical checks:
1. Determines if a write was already in progress when the read began (odd before_cc value)
2. Compares the initial and final counter values to detect if any writes started and completed during the read

The function returns true only if the read was consistent (no concurrent writes), otherwise false to indicate the read should be retried.

## Parameters / Member Variables
- `cc`: Pointer to a 32-bit change counter that tracks the state of write operations
- `before_cc`: The change counter value captured at the beginning of the read operation (from pgstat_begin_changecount_read)

## Dependencies
- Functions called/Symbols referenced:
  - pg_read_barrier (memory barrier function)
- Called from (representative examples):
  - [pgstat_copy_changecounted_stats](pgstat_copy_changecounted_stats.md)

## Notes and Other Information
- This function must always be paired with a preceding pgstat_begin_changecount_read call
- Returns false if before_cc was odd (write was in progress when read started)
- Returns false if before_cc != after_cc (writes occurred during the read)
- The read barrier ensures that all data reads complete before checking the final counter value
- This implements a classic optimistic concurrency control mechanism
- Readers must retry their entire read operation when this function returns false
- The protocol ensures readers never see partially written or inconsistent data

## Simplified Source

```c
static inline bool pgstat_end_changecount_read(uint32 *cc, uint32 before_cc) {
    // Memory barrier ensures all data reads complete before counter check
    pg_read_barrier();

    // Capture change counter value after reading data
    uint32 after_cc = *cc;

    // Check if write was in progress when read started (odd counter)
    if (before_cc & 1)
        return false;

    // Check if counter changed during read (writes occurred)
    return before_cc == after_cc;
}
```

**Key Points:**
- Validates consistency of statistics read operation
- Returns false if write was in progress when read started
- Returns false if any writes occurred during the read
- Must be paired with pgstat_begin_changecount_read
- Implements optimistic concurrency control for lock-free reading