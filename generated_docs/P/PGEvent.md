# PGEvent

## Location
src/interfaces/libpq/libpq-int.h: 161 - 168

## Overview
A structure that represents a registered event handler in PostgreSQL's libpq event system, storing callback functions and associated data for handling connection and result events.

## Definition


## Detailed Description
The `PGEvent` structure is a core component of libpq's event system, which allows applications to register callback functions that are notified when certain events occur during database operations. Events include connection establishment, connection destruction, result creation, and result destruction.

Each registered event handler is represented by a `PGEvent` structure that contains the callback function pointer, associated data, and metadata for managing the event handler's lifecycle. The event system allows applications to extend libpq's functionality and perform custom processing during database operations.

## Parameters / Member Variables
- `proc`: Function pointer to the event callback function (`PGEventProc` type)
- `name`: String identifier for the event handler, used primarily in error messages
- `passThrough`: User-provided pointer passed to the callback function, set during registration
- `data`: Optional instance-specific data associated with this event handler
- `resultInitialized`: Boolean flag indicating whether RESULTCREATE/COPY events succeeded

## Dependencies
- Functions called/Symbols referenced:
  - PGEventProc (function pointer type for event callbacks)
- Called from (representative examples):
  - `[PQregisterEventProc](PQregisterEventProc.md)()` - registers new event handlers
  - `[dupEvents](../d/dupEvents.md)()` - duplicates event handlers when copying results
  - Used in `pg_result` and `pg_conn` structures for event management

## Notes and Other Information
- Part of libpq's extensible event system allowing applications to hook into database operations
- Event handlers are registered per connection and can be associated with specific results
- The `resultInitialized` flag tracks whether result-related events were successfully initialized
- Event handlers are called for various events: connection creation/destruction, result creation/destruction
- The `passThrough` pointer allows applications to pass context data to their event handlers
- Memory management for the `name` field and associated data is handled by the event system
- Event handlers are automatically called during appropriate operations and can modify or react to database operations