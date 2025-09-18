# PGEventConnDestroy

## Location
src/interfaces/libpq/libpq-events.h: 50 - 55

## Overview
PGEventConnDestroy is a structure that contains event information passed to event callback functions when a PGEVT_CONNDESTROY event is fired before a connection is destroyed.

## Definition


## Detailed Description
PGEventConnDestroy is used as the event information structure when the libpq event system fires a PGEVT_CONNDESTROY event. This event occurs when a PostgreSQL connection is being destroyed, typically during cleanup operations such as PQfinish() or when a connection object is being freed. The structure provides event callbacks with access to the connection object that is about to be destroyed, allowing them to perform any necessary cleanup operations, release resources, or save state before the connection becomes invalid.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object that is being destroyed

## Dependencies
- Functions called/Symbols referenced:
  - PGconn (PostgreSQL connection structure)
- Called from (representative examples):
  - [freePGconn](../f/freePGconn.md) (creates and passes this structure to event callbacks at fe-connect.c:4639)

## Notes and Other Information
- This structure is specifically used for PGEVT_CONNDESTROY events and is passed to event callback functions as the evtInfo parameter
- The event is fired before the connection is actually destroyed, giving callbacks a final opportunity to access connection data and perform cleanup
- Event callbacks should use this event to release any connection-specific resources they may have allocated during the connection's lifetime
- This is the last event that will be fired for a connection, so callbacks should not expect any further events after receiving this one
- The structure is created and populated in freePGconn() just before the connection cleanup process begins
- After this event is processed, the connection object will become invalid and should not be used