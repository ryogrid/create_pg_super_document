# DisableTimeoutParams

## Location
[src/include/utils/timeout.h:73-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timeout.h#L73-L96)

## Overview
DisableTimeoutParams is a structure used to specify parameters when disabling multiple timeouts at once in PostgreSQL's timeout management system.

## Definition

```c
typedef struct
{
	TimeoutId	id;				/* timeout to clear */
	bool		keep_indicator; /* keep the indicator flag? */
} DisableTimeoutParams;
```
## Detailed Description
DisableTimeoutParams serves as a parameter structure for the disable_timeouts() function, enabling efficient batch disabling of multiple timeout types. This structure provides fine-grained control over timeout deactivation by allowing callers to specify whether timeout indicator flags should be preserved when disabling timeouts. The structure is part of PostgreSQL's comprehensive timeout management system that multiplexes SIGALRM interrupts for various timeout scenarios.

## Parameters / Member Variables
- `id`: The TimeoutId identifying which specific timeout to disable
- `keep_indicator`: Boolean flag indicating whether to preserve the timeout's indicator flag after disabling; when true, the indicator remains set even after timeout deactivation, when false, the indicator is cleared along with the timeout

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md) (enumeration type)
- Called from (representative examples):
  - disable_timeouts (timeout.c:718)
  - [LockErrorCleanup](../L/LockErrorCleanup.md) (proc.c:738)
  - ProcSleep (proc.c:1623)

## Notes and Other Information
The structure is designed for batch operations where multiple timeouts need to be disabled simultaneously with different indicator preservation requirements. The keep_indicator flag provides flexibility in timeout management - preserving indicators allows other parts of the system to detect that a timeout occurred even after it has been disabled, which is useful for cleanup operations and error handling scenarios. This structure complements EnableTimeoutParams for comprehensive timeout lifecycle management.