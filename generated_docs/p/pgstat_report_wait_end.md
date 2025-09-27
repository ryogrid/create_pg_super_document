# pgstat_report_wait_end

## Location
[src/include/utils/wait_event.h:101-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/wait_event.h#L101-L108)

## Overview
Reports the end of a wait event by clearing the wait event information, indicating that the server process is no longer waiting.

## Definition

```c
static inline void
pgstat_report_wait_end(void)
```
## Detailed Description
This function is called to report the end of a wait event. It clears the wait event information by setting the  pointer to 0, indicating that the process is no longer in a waiting state.

The function performs an atomic write to clear the wait event status, complementing . Like its counterpart, the write operation is atomic since it's always performed as a 4-byte operation.

This function is typically called immediately after the corresponding wait condition is resolved, such as after completing I/O operations, acquiring locks, or finishing other blocking operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (global pointer variable)

- Called from (representative examples):
  -  (after logical rewrite operations)
  -  (after transaction status updates)
  - / (after SLRU I/O operations)
  -  (after WAL writing operations)
  - / (after file I/O operations)
  -  (after event waiting completes)
  -  (after lightweight lock wait ends)
  - / (during transaction cleanup)
  - Various background processes and auxiliary processes
  - Many other completion points throughout the codebase

## Notes and Other Information
- This is a static inline function defined in the header for performance
- Always used in pairs with  to bracket wait periods
- The function is extremely lightweight since it's called frequently
- Clearing the wait event (setting to 0) immediately makes the process appear as "not waiting" in system activity views
- Essential for accurate wait event monitoring and performance analysis
- The function safely handles calls at any point in the backend lifecycle

## Simplified Source

```c
// Simplified version of pgstat_report_wait_end
static inline void
pgstat_report_wait_end(void) {
    // Clear wait event info: Set to 0 to indicate process is no longer waiting
    *(volatile uint32 *) my_wait_event_info = 0;
}
```

Key simplifications made:
- Added explanatory comment for the core operation
- The function is already extremely simple with just one operation
- Preserved the volatile pointer access which is essential for correct behavior
- No error handling or complex logic to simplify - this is a minimal atomic operation