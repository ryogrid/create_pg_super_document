# PQping

## Location
src/interfaces/libpq/fe-connect.c: 760 - 790

## Overview
Checks the status of a PostgreSQL server connection without establishing a persistent connection, using the same connection parameters as PQconnectdb.

## Definition
```c
PGPing PQping(const char *conninfo)
```

## Detailed Description
PQping is a utility function that determines whether a PostgreSQL server is running and accessible without creating a persistent connection. It works by initiating a connection using PQconnectStart, then immediately testing the connection status through internal_ping, and finally cleaning up the temporary connection with PQfinish. This function is useful for health checks, monitoring, and determining server availability before attempting actual database operations. The return value indicates the specific ping result status.

## Parameters / Member Variables
- `conninfo`: A connection string containing database connection parameters, using the same format as PQconnectdb (either key=value pairs or URI format)

## Dependencies
- Functions called/Symbols referenced:
  - [PQconnectStart](PQconnectStart.md)
  - [internal_ping](../i/internal_ping.md)
  - [PQfinish](PQfinish.md)
  - PGPing (return type enum)
- Called from (representative examples):
  - Referenced in libpq-fe.h header definitions

## Notes and Other Information
- Returns a PGPing enum value indicating the ping result status
- Does not create a persistent connection - automatically cleans up after checking
- Uses the same connection parameter format as PQconnectdb for consistency
- Ideal for monitoring and health check scenarios where you only need to verify server availability
- More lightweight than establishing a full connection when you only need to test connectivity
- The function handles connection cleanup internally, so callers do not need to manage connection resources