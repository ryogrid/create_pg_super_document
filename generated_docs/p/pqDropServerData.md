# pqDropServerData

## Location
src/interfaces/libpq/fe-connect.c: 584 - 688

## Overview
Clears all connection state data that was received from or deduced about the PostgreSQL server, essential for preparing connections between different servers.

## Definition
```c
static void pqDropServerData(PGconn *conn)
```

## Detailed Description
This function performs a comprehensive cleanup of server-specific state information while preserving connection parameters needed for establishing new connections. Unlike pqDropConnection which handles the physical connection teardown, pqDropServerData focuses on clearing logical state data that could interfere with connections to different servers.

The function resets various categories of server state including pending notifications, server parameters, encoding information, authentication state, and server version details. It is designed to be called when starting a new connection attempt, ensuring no stale data from previous server interactions affects the new connection.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object (PGconn) whose server data should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - PGnotify (struct type)
  - pgParameterStatus (struct type)
  - PG_SQL_ASCII (constant)
  - PG_BOOL_UNKNOWN (constant)
  - SCRAM_SHA_256_DEFAULT_ITERATIONS (constant)
  - free (standard library function)

- Called from (representative examples):
  - PQconnectPoll
  - pqClosePGconn

## Notes and Other Information
- The function preserves be_pid and be_key for cancel connections to maintain access to the secret token needed for cancellation operations
- Resets client encoding to PG_SQL_ASCII and various boolean parameters to unknown state
- Clears notification queue, parameter status list, and large object function lookup data
- Designed to be separate from pqDropConnection to allow for different timing of physical vs logical connection cleanup
- Essential for proper connection reuse and switching between different PostgreSQL servers
- Handles memory management for dynamically allocated notification and parameter status structures