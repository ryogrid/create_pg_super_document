# PQresultInstanceData

## Location
[src/interfaces/libpq/libpq-events.c:165-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.c#L165-L184)

## Overview
Retrieves the instance-specific data associated with a registered event procedure within a PostgreSQL result object.

## Definition
```c
void *PQresultInstanceData(const PGresult *result, PGEventProc proc)
```

## Detailed Description
PQresultInstanceData provides access to the instance data previously stored for an event procedure using PQresultSetInstanceData. Operating on PGresult objects rather than PGconn objects, this function searches through the registered event procedures in the given result to find the one matching the provided procedure pointer and returns its associated data. This allows event procedures to retrieve their result-specific state information. The function is read-only and does not modify the result state.

## Parameters / Member Variables
- `result`: The PGresult object to search for the event procedure
- `proc`: The event procedure function pointer used to identify which event data to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - None (simple array search)
- Called from (representative examples):
  - PGEventResultDestroy (libpq-events.h:85)

## Notes and Other Information
Returns the data pointer associated with the event procedure, or NULL if the procedure is not found or no data has been set. The function accepts const parameters, indicating it does not modify the result object. Applications should check for NULL return values before using the returned pointer. This is the result-specific counterpart to PQinstanceData for connection objects.