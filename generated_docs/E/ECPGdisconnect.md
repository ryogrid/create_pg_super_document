# ECPGdisconnect

## Location
[src/interfaces/ecpg/ecpglib/connect.c:678-721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/connect.c#L678-L721)

## Overview
ECPGdisconnect closes database connections in ECPG, supporting both individual named connections and bulk disconnection of all connections simultaneously.

## Definition
```c
bool ECPGdisconnect(int lineno, const char *connection_name)
```

## Detailed Description
ECPGdisconnect handles the termination of PostgreSQL database connections in ECPG applications. The function supports two operation modes: disconnecting a specific named connection or disconnecting all active connections when "ALL" is specified as the connection name. It provides thread-safe operation using mutex locks and properly cleans up connection resources including prepared statements, cached results, and the underlying PostgreSQL connection. The function validates connections before attempting disconnection and ensures proper SQLCA initialization for error reporting.

## Parameters / Member Variables
- `lineno`: Source code line number where disconnection is requested, used for error reporting and debugging
- `connection_name`: Name of the connection to disconnect, or "ALL" to disconnect all active connections

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - [ecpg_raise](../e/ecpg_raise.md)
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)/unlock
  - [ecpg_init_sqlca](../e/ecpg_init_sqlca.md)
  - [ecpg_get_connection_nr](../e/ecpg_get_connection_nr.md)
  - [ecpg_init](../e/ecpg_init.md)
  - [ecpg_finish](../e/ecpg_finish.md)
- Called from (representative examples):
  - ECPG-generated code for connection cleanup
  - Test programs and applications using ECPG

## Notes and Other Information
- Returns true on successful disconnection, false on failure
- Special "ALL" connection name disconnects all active connections in the global list
- Thread-safe implementation with mutex protection for connection list access
- Automatically handles cleanup of prepared statements and cached query results
- Validates connection existence and state before attempting disconnection
- Part of ECPGs connection lifecycle management infrastructure
- Uses ecpg_finish() to perform the actual connection cleanup and resource deallocation
- Maintains SQLCA state for proper error reporting to the application