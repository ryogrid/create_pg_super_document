# PQresultSetInstanceData

## Location
[src/interfaces/libpq/libpq-events.c:142-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.c#L142-L164)

## Overview
Sets instance-specific data for a registered event procedure within a PostgreSQL result object.

## Definition
```c
int PQresultSetInstanceData(PGresult *result, PGEventProc proc, void *data)
```

## Detailed Description
PQresultSetInstanceData allows applications to associate custom data with a previously registered event procedure on a specific result object. Similar to PQsetInstanceData but operating on PGresult objects rather than PGconn objects, this function enables event procedures to maintain result-specific state information. The function searches through the events array in the result object to find the matching procedure and updates its data field. This is particularly useful for tracking information related to specific query results.

## Parameters / Member Variables
- `result`: The PGresult object containing the registered event procedure
- `proc`: The event procedure function pointer used to identify which event to update
- `data`: The instance-specific data pointer to associate with the event procedure

## Dependencies
- Functions called/Symbols referenced:
  - None (simple array search and assignment)
- Called from (representative examples):
  - PGEventResultDestroy (libpq-events.h:82)

## Notes and Other Information
The event procedure must have been previously registered and copied to the result object before instance data can be set. Returns true on success, false if the procedure is not found or invalid parameters are provided. The data pointer is stored as-is without copying, so the caller must ensure the data remains valid for the lifetime of the result object.

## Simplified Source

```c
int PQresultSetInstanceData(PGresult *result, PGEventProc proc, void *data) {
    int i;

    // Validate parameters
    if (!result || !proc)
        return false;

    // Find the matching event procedure in result
    for (i = 0; i < result->nEvents; i++) {
        if (result->events[i].proc == proc) {
            // Set the instance data
            result->events[i].data = data;
            return true;
        }
    }

    // Event procedure not found
    return false;
}
```