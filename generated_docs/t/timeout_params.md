# timeout_params

## Location
[src/backend/utils/misc/timeout.c:26-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L26-L40)

## Overview
The  struct stores configuration and state information for a single timeout mechanism in PostgreSQL's timeout management system, including timing details, callback handlers, and activation status.

## Definition

```c
typedef struct timeout_params
{
	TimeoutId	index;			/* identifier of timeout reason */

	/* volatile because these may be changed from the signal handler */
	volatile bool active;		/* true if timeout is in active_timeouts[] */
	volatile bool indicator;	/* true if timeout has occurred */

	/* callback function for timeout, or NULL if timeout not registered */
	timeout_handler_proc timeout_handler;

	TimestampTz start_time;		/* time that timeout was last activated */
	TimestampTz fin_time;		/* time it is, or was last, due to fire */
	int			interval_in_ms; /* time between firings, or 0 if just once */
} timeout_params;
```
## Detailed Description
The  structure is the core data structure for PostgreSQL's unified timeout management system. Each instance represents one timeout reason (such as deadlock detection, statement timeout, or connection timeout) and contains all necessary information to track its state and execute its callback when the timeout expires.

The structure is designed to be signal-safe, with volatile fields that can be modified from signal handlers. PostgreSQL maintains a global array  of these structures, indexed by  enum values, and a separate  array that tracks currently active timeouts sorted by their expiration time.

The timeout system uses SIGALRM signals and supports both one-shot and repeating timeouts. The structure enables efficient timeout scheduling by storing both absolute expiration times and repeat intervals.

## Parameters / Member Variables
- : The  enum value that uniquely identifies this timeout reason (e.g., DEADLOCK_TIMEOUT, STATEMENT_TIMEOUT)
- : Volatile boolean indicating whether this timeout is currently in the active_timeouts[] array and scheduled to fire
- : Volatile boolean flag set to true when the timeout has actually occurred, used by timeout handlers to check if their timeout fired
- : Function pointer to the callback function executed when this timeout fires, or NULL if the timeout is not currently registered
- : The  when this timeout was last activated or rescheduled
- : The absolute  when this timeout is scheduled to expire (or did expire for completed timeouts)
- : For repeating timeouts, the interval in milliseconds between firings; 0 for one-shot timeouts

## Dependencies
- Functions called/Symbols referenced:
  -  (enum type for timeout identification)
  -  (function pointer type for callbacks)
  -  (timestamp type from datatype/timestamp.h)
  
- Called from (representative examples):
  -  (configures timeout parameters when activating a timeout)
  -  (accesses timeout parameters during signal handling)

## Notes and Other Information
- The  and  fields are marked volatile because they are accessed and modified from signal handlers, requiring careful synchronization
- The structure supports both PostgreSQL's predefined timeout types (deadlock, statement, etc.) and user-definable timeout reasons
- Timeouts are prioritized by their  value when multiple timeouts have the same expiration time
- The design allows for efficient timeout management with O(n) insertion and removal from the active timeout list
- Signal safety is crucial since timeout handling occurs in interrupt context via SIGALRM
- The  field stores absolute timestamps rather than relative delays, enabling precise timeout scheduling regardless of when the timeout was set