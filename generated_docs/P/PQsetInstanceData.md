# PQsetInstanceData

## Location
[src/interfaces/libpq/libpq-events.c:98-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.c#L98-L120)

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

## Simplified Source

```c
int PQsetInstanceData(PGconn *conn, PGEventProc proc, void *data) {
    int i;

    // Validate parameters
    if (!conn || !proc)
        return false;

    // Find the matching event procedure
    for (i = 0; i < conn->nEvents; i++) {
        if (conn->events[i].proc == proc) {
            // Set the instance data
            conn->events[i].data = data;
            return true;
        }
    }

    // Event procedure not found
    return false;
}
```