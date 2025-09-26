# pgstat_report_wait_start

## Location
[src/include/utils/wait_event.h:85-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/wait_event.h#L85-L100)

## Overview
Reports the start of a wait event by storing wait event information that can be monitored by system activity tracking.

## Definition

```c
static inline void
pgstat_report_wait_start(uint32 wait_event_info)
```
## Detailed Description
This function is called from locations where a server process needs to wait, such as I/O operations, locks, or other blocking operations. It stores wait event information as a 4-byte value where:
- First byte represents the wait event class (type of wait - see WaitClass enum)
- Next 3 bytes represent the actual wait event (currently 2 bytes are used, 1 byte reserved for future)

The function performs an atomic write to the  pointer, which initially points to local memory during backend startup, making it safe to call before  has been initialized. The reporting is unconditional (not dependent on ) as the overhead of checking that setting was found to be higher than the cost of always reporting.

Since the field is always read and written as a 4-byte value, updates are atomic on most architectures.

## Parameters / Member Variables
- : A 32-bit value encoding both the wait event class and the specific wait event identifier

## Dependencies
- Functions called/Symbols referenced:
  -  (global pointer variable)

- Called from (representative examples):
  -  (during logical rewrite operations)
  -  (during transaction status updates) 
  - / (SLRU I/O operations)
  -  (WAL writing operations)
  - / (file I/O operations)
  -  (event waiting)
  -  (lightweight lock waits)
  - Many other I/O and synchronization points throughout the codebase

## Notes and Other Information
- This is a static inline function defined in the header for performance
- The function is designed to be extremely lightweight since it's called frequently
- The wait event information can be viewed through system views like 
- Historically this reporting was conditional on , but that check was removed for performance reasons
- The function safely handles early calls before backend initialization is complete