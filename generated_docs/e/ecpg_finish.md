# ecpg_finish

## Location
[src/interfaces/ecpg/ecpglib/connect.c:108-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/connect.c#L108-L157)

## Overview
Performs complete cleanup and termination of an ECPG database connection, including deallocating prepared statements, closing the PostgreSQL connection, and updating connection tracking structures.

## Definition
```c
static void ecpg_finish(struct connection *act)
```

## Detailed Description
This function implements comprehensive connection cleanup for the ECPG library. It performs several critical operations: deallocates all prepared statements associated with the connection, closes the underlying PostgreSQL connection using PQfinish(), removes the connection from the global connection list, updates thread-specific and global connection pointers if they referenced this connection, frees the connection's type information cache, and cleans up cursor variables when the last connection is closed. The function is designed to be called while holding the connections_mutex to ensure thread safety.

## Parameters / Member Variables
- `act`: Pointer to the connection structure to be terminated and cleaned up. If NULL, the function logs a message and returns without action.

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_deallocate_all_conn](ecpg_deallocate_all_conn.md) (cleans up prepared statements)
  - [PQfinish](../P/PQfinish.md) (closes PostgreSQL connection)
  - `pthread_getspecific` (checks thread-specific connection)
  - `pthread_setspecific` (updates thread-specific connection)
  - [ecpg_log](ecpg_log.md) (logs connection closure)
  - `ecpg_free` (frees allocated memory)
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md) (on connection failure)
  - [ECPGdisconnect](../E/ECPGdisconnect.md) (for explicit disconnection)

## Notes and Other Information
- This is a static function, only accessible within connect.c
- Assumes the caller holds connections_mutex for thread safety
- Performs fallback connection updates: if the closed connection was the current one (either thread-specific or global), it sets the current connection to the first remaining connection
- Cleans up the ECPGtype_information_cache linked list to prevent memory leaks
- When the last connection is closed (`all_connections == NULL`), it also cleans up the cursor variables list (`ivlist`)
- Uses ECPG_COMPAT_PGSQL mode for prepared statement deallocation
- Logs connection closure events for debugging purposes
- Part of ECPG's connection management infrastructure ensuring proper resource cleanup