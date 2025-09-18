# PQpingParams

## Location
src/interfaces/libpq/fe-connect.c: 707 - 743

## Overview
Checks PostgreSQL server status and connectivity using connection parameters provided in keyword-value arrays, without establishing a persistent connection.

## Definition
```c
PGPing PQpingParams(const char *const *keywords, const char *const *values, int expand_dbname)
```

## Detailed Description
This function provides a lightweight way to test PostgreSQL server connectivity and availability using the same parameter format as PQconnectdbParams. It establishes a temporary connection to determine server status and immediately closes it, making it ideal for health checks and monitoring applications.

The function internally uses PQconnectStartParams to initiate a connection attempt, then calls internal_ping to determine the server's status, and finally cleans up the connection with PQfinish. This approach allows for comprehensive connectivity testing without the overhead of maintaining a persistent connection.

## Parameters / Member Variables
- `keywords`: NULL-terminated array of connection parameter names (e.g., "host", "port", "dbname")
- `values`: NULL-terminated array of corresponding parameter values, parallel to keywords array  
- `expand_dbname`: Integer flag indicating whether to expand the database name if it appears to be a connection URI or connection string

## Dependencies
- Functions called/Symbols referenced:
  - [PQconnectStartParams](PQconnectStartParams.md)
  - [internal_ping](../i/internal_ping.md)
  - [PQfinish](PQfinish.md)
  - PGPing (return type enum)

- Called from (representative examples):
  - pg_isready utility
  - [regression_main](../r/regression_main.md) (PostgreSQL regression tests)

## Notes and Other Information
- Returns a PGPing enum value indicating server status (PQPING_OK, PQPING_REJECT, PQPING_NO_RESPONSE, PQPING_NO_ATTEMPT)
- Automatically handles connection cleanup, making it safe for repeated use in monitoring scenarios
- More efficient than establishing a full connection when only connectivity testing is needed
- Commonly used in health check scripts and monitoring tools
- Part of the libpq public API for server availability testing
- The connection attempt may involve network timeouts and authentication, so it should be used with appropriate timeout considerations