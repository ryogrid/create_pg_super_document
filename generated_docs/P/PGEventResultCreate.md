# PGEventResultCreate

## Location
[src/interfaces/libpq/libpq-events.h:56-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.h#L56-L61)

## Overview
PGEventResultCreate is a structure that contains event information passed to event callback functions when a PGEVT_RESULTCREATE event is fired after a new PGresult object is created.

## Definition


## Detailed Description
PGEventResultCreate is used as the event information structure when the libpq event system fires a PGEVT_RESULTCREATE event. This event occurs when a new PGresult object is created, typically after executing a query or command. The structure provides event callbacks with access to both the connection object that generated the result and the newly created result object itself. This allows callbacks to perform initialization operations on the result, attach additional data, or perform tracking operations for result lifecycle management.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object that generated the result
- `result`: Pointer to the newly created PGresult object

## Dependencies
- Functions called/Symbols referenced:
  - PGconn (PostgreSQL connection structure)
  - PGresult (PostgreSQL result structure)
- Called from (representative examples):
  - [PQfireResultCreateEvents](PQfireResultCreateEvents.md) (creates and passes this structure to event callbacks at libpq-events.c:198)
  - [fe](../f/fe.md)-exec.c:2214 (calls PQfireResultCreateEvents to trigger the event)

## Notes and Other Information
- This structure is specifically used for PGEVT_RESULTCREATE events and is passed to event callback functions as the evtInfo parameter
- The event is fired once for each newly created result object, allowing callbacks to initialize any result-specific data or resources
- The structure provides access to both the connection and result objects, enabling callbacks to establish relationships between them
- Event callbacks can use this event to attach instance data to the result using PQresultSetInstanceData()
- The event is only fired once per result object, and a flag (resultInitialized) is used to prevent duplicate firing
- If an event callback returns false during PGEVT_RESULTCREATE, it indicates a failure and the overall operation may be considered unsuccessful