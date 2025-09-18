# PQsetInstanceData

## Location
src/interfaces/libpq/libpq-events.c: 98 - 120

## Overview
Sets instance-specific data for a registered event procedure within a PostgreSQL connection.

## Definition
```c
int PQsetInstanceData(PGconn *conn, PGEventProc proc, void *data)
```

## Detailed Description
PQsetInstanceData allows applications to associate custom data with a previously registered event procedure on a specific connection. This instance data is stored in the events array and can be retrieved later using PQinstanceData. The function searches through the registered event procedures to find the one matching the provided procedure pointer and updates its data field. This mechanism enables event procedures to maintain state information specific to each connection.

## Parameters / Member Variables
- `conn`: The PGconn connection object containing the registered event procedure
- `proc`: The event procedure function pointer used to identify which event to update
- `data`: The instance-specific data pointer to associate with the event procedure

## Dependencies
- Functions called/Symbols referenced:
  - None (simple array search and assignment)
- Called from (representative examples):
  - PGEventResultDestroy (libpq-events.h:76)

## Notes and Other Information
The event procedure must have been previously registered using PQregisterEventProc before instance data can be set. Returns true on success, false if the procedure is not found or invalid parameters are provided. The data pointer is stored as-is without copying, so the caller must ensure the data remains valid for the lifetime of the connection.