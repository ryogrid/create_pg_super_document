# PQinstanceData

## Location
src/interfaces/libpq/libpq-events.c: 121 - 141

## Overview
Retrieves the instance-specific data associated with a registered event procedure within a PostgreSQL connection.

## Definition
```c
void *PQinstanceData(const PGconn *conn, PGEventProc proc)
```

## Detailed Description
PQinstanceData provides access to the instance data previously stored for an event procedure using PQsetInstanceData. The function searches through the registered event procedures on the given connection to find the one matching the provided procedure pointer and returns its associated data. This allows event procedures to retrieve their connection-specific state information. The function is read-only and does not modify the connection state.

## Parameters / Member Variables
- `conn`: The PGconn connection object to search for the event procedure
- `proc`: The event procedure function pointer used to identify which event data to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - None (simple array search)
- Called from (representative examples):
  - PGEventResultDestroy (libpq-events.h:79)

## Notes and Other Information
Returns the data pointer associated with the event procedure, or NULL if the procedure is not found or no data has been set. The function accepts const parameters, indicating it does not modify the connection. Applications should check for NULL return values before using the returned pointer.