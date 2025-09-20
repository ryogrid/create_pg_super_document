# finishCon

## Location
[src/bin/pgbench/pgbench.c:7731-7746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7731-L7746)

## Overview
A utility function that safely closes and cleans up a PostgreSQL database connection for a pgbench client state.

## Definition

```c
static void
finishCon(CState *st)
```
## Detailed Description
This function provides a safe wrapper around PostgreSQL's PQfinish() function to properly close database connections associated with pgbench client states. It includes a null pointer check to prevent attempting to close already-closed or uninitialized connections, and sets the connection pointer to NULL after closing to prevent double-close scenarios. This function is essential for proper resource cleanup in pgbench's connection management system.

## Parameters / Member Variables
- : Pointer to CState structure representing a client connection state whose database connection should be closed

## Dependencies
- Functions called/Symbols referenced:
  - [PQfinish](../P/PQfinish.md) (PostgreSQL libpq function for closing connections)
  - [CState](../C/CState.md) (client state structure type)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (at lines 4245 and 4286)
  - [disconnect_all](../d/disconnect_all.md) (at line 4725)

## Notes and Other Information
- Performs null check before calling PQfinish to avoid segmentation faults
- Sets connection pointer to NULL after closing for safety
- Part of pgbench's connection lifecycle management
- Used during error handling, normal connection cleanup, and thread termination
- Ensures proper resource cleanup to prevent connection leaks