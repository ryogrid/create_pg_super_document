# PGEventRegister

## Location
src/interfaces/libpq/libpq-events.h: 40 - 44

## Overview
PGEventRegister is a structure that contains event information passed to event callback functions when a PGEVT_REGISTER event is fired during event processor registration.

## Definition


## Detailed Description
PGEventRegister is used as the event information structure when the libpq event system fires a PGEVT_REGISTER event. This event occurs when an application registers an event callback function with a PostgreSQL connection using PQregisterEventProc(). The structure provides the event callback with access to the connection object that the event processor is being registered with, allowing the callback to perform any necessary initialization or setup operations.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection object that the event processor is being registered with

## Dependencies
- Functions called/Symbols referenced:
  - PGconn (PostgreSQL connection structure)
- Called from (representative examples):
  - PQregisterEventProc (creates and passes this structure to event callbacks)

## Notes and Other Information
- This structure is specifically used for PGEVT_REGISTER events and is passed to event callback functions as the evtInfo parameter
- The event callback receives this structure when it's first registered, allowing it to initialize any connection-specific data or resources
- The structure is created and populated by PQregisterEventProc() at line 82 in libpq-events.c before calling the event callback
- Event callbacks should check the PGEventId parameter to determine the event type before casting evtInfo to PGEventRegister