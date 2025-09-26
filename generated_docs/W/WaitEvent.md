# WaitEvent

## Location
[src/include/storage/latch.h:152-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/latch.h#L152-L161)

## Overview
WaitEvent is a structure that represents individual events returned from PostgreSQL's wait event system, containing information about triggered I/O, latch, or timeout conditions during event-driven waiting operations.

## Definition

```c
typedef struct WaitEvent
{
	int			pos;			/* position in the event data structure */
	uint32		events;			/* triggered events */
	pgsocket	fd;				/* socket fd associated with event */
	void	   *user_data;		/* pointer provided in AddWaitEventToSet */
#ifdef WIN32
	bool		reset;			/* Is reset of the event required? */
#endif
} WaitEvent;
```
## Detailed Description
WaitEvent structures are returned by PostgreSQL's event waiting infrastructure to indicate which specific events have occurred during a wait operation. These events can include latch signaling, socket readiness (readable/writable), timeout expiration, or postmaster death detection.

The WaitEvent system is designed around the concept of event sets (WaitEventSet) where multiple events can be monitored simultaneously. When one or more events occur, they are returned as an array of WaitEvent structures, each describing a specific triggered condition.

This mechanism supports PostgreSQL's asynchronous I/O operations, background worker coordination, client-server communication, and internal process synchronization. The system abstracts over different platform-specific event notification mechanisms like epoll (Linux), kqueue (BSD), poll (POSIX), and Win32 events.

## Parameters / Member Variables
- : Position/index of this event within the WaitEventSet's events array, used for internal tracking and management
- : Bitmask indicating which specific events were triggered (e.g., WL_LATCH_SET, WL_SOCKET_READABLE, WL_SOCKET_WRITABLE, WL_TIMEOUT, WL_POSTMASTER_DEATH)
- : Socket file descriptor associated with this event, set to PGINVALID_SOCKET for non-socket events like latches
- : Opaque pointer provided when the event was added to the set via AddWaitEventToSet, allows callers to associate custom data with events
-  (Windows only): Boolean flag indicating whether the underlying Win32 event object needs to be reset after processing

## Dependencies
- Functions called/Symbols referenced:
  - pgsocket (cross-platform socket type)
  - PGINVALID_SOCKET (invalid socket constant)
  - Various WL_* event type constants

- Called from (representative examples):
  - [WaitEventSetWait](WaitEventSetWait.md) (main event waiting function)
  - [WaitLatch](WaitLatch.md) (latch-specific waiting)
  - [WaitLatchOrSocket](WaitLatchOrSocket.md) (combined latch and socket waiting)
  - [ServerLoop](../S/ServerLoop.md) (postmaster main loop)
  - [WalSndWait](WalSndWait.md) (WAL sender waiting)
  - Various executor and libpq functions for async operations

## Key Functions Operating on WaitEvent
- : Main function that returns array of triggered WaitEvent structures
- : Creates event set that can generate WaitEvents
- : Adds events that will generate WaitEvents when triggered
- : Modifies existing events in the set
- : Convenience function that internally uses WaitEvent system

## Event Types (events field values)
- : Latch has been signaled
- : Socket has data available for reading
- : Socket is ready for writing
- : Timeout period has elapsed
- : Postmaster process has died
- : Exit immediately on postmaster death

## Notes and Other Information
- [WaitEvent](WaitEvent.md) structures are typically allocated as arrays to receive multiple simultaneous events
- The pos field allows efficient mapping back to the original event registration in the WaitEventSet
- user_data provides a way for applications to associate context with events without additional lookups
- On Windows, the reset field handles platform-specific event object management requirements
- Events can be combined (bitwise OR) when multiple conditions are satisfied simultaneously
- The system is designed to be interrupt-safe and can handle signal delivery during wait operations
- Cross-platform abstraction allows the same WaitEvent interface to work across Unix and Windows systems