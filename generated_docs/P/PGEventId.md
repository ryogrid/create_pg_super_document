# PGEventId

## Location
src/interfaces/libpq/libpq-events.h: 35 - 39

## Overview
PGEventId is an enumeration that defines callback event identifiers used in PostgreSQL's libpq event system to notify applications about various connection and result lifecycle events.

## Definition


## Detailed Description
PGEventId serves as the event type identifier in PostgreSQL's libpq event notification system. This enumeration is used to specify which type of event has occurred when an event callback function (PGEventProc) is invoked. The event system allows applications to register callback functions that get notified during various stages of connection and result object lifecycles, enabling custom resource management, logging, or cleanup operations.

## Parameters / Member Variables
- : Event fired when an event processor is first registered with a connection
- : Event fired when a connection is reset (e.g., via PQreset)
- : Event fired when a connection is being destroyed
- : Event fired when a new PGresult object is created
- : Event fired when a PGresult object is copied
- : Event fired when a PGresult object is being destroyed

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration definition)
- Called from (representative examples):
  - PGEventProc (as parameter to event callback functions)
  - [PQregisterEventProc](PQregisterEventProc.md) (used in event registration)

## Notes and Other Information
- This enumeration is part of the libpq events API, which is primarily used by applications that need to track connection and result lifecycles
- Each event type corresponds to a specific struct type (PGEventRegister, PGEventConnReset, etc.) that provides context-specific information
- The event system is optional and only used when applications explicitly register event callbacks
- Event callbacks receive the PGEventId to determine which type of event occurred and how to interpret the accompanying event information