# PGEventConnReset

## Location
[src/interfaces/libpq/libpq-events.h:45-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.h#L45-L49)

## Overview
PGEventConnReset is a structure that contains event information passed to event callback functions when a PGEVT_CONNRESET event is fired after a successful connection reset operation.

## Definition

```c
typedef struct
{
	PGconn	   *conn;
} PGEventConnDestroy;
```
## Detailed Description
PGEventConnReset is used as the event information structure when the libpq event system fires a PGEVT_CONNRESET event. This event occurs after a PostgreSQL connection has been successfully reset using either PQreset() or PQresetPoll(). The structure provides event callbacks with access to the connection object that was reset, allowing them to perform any necessary cleanup, re-initialization, or state synchronization operations that may be required after a connection reset.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object that has been successfully reset

## Dependencies
- Functions called/Symbols referenced:
  - PGconn (PostgreSQL connection structure)
- Called from (representative examples):
  - [PQreset](PQreset.md) (creates and passes this structure to event callbacks at fe-connect.c:4907)
  - [PQresetPoll](PQresetPoll.md) (creates and passes this structure to event callbacks at fe-connect.c:4959)

## Notes and Other Information
- This structure is specifically used for PGEVT_CONNRESET events and is passed to event callback functions as the evtInfo parameter
- The event is only fired after a successful connection reset, not during failed reset attempts
- Event callbacks receive this structure when a connection reset completes successfully, allowing them to reinitialize any connection-specific resources or state
- Both synchronous (PQreset) and asynchronous (PQresetPoll) reset operations trigger this event
- The structure is created and populated just before calling registered event callbacks to notify them of the successful reset