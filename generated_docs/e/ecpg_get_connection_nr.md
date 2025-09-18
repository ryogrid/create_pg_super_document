# ecpg_get_connection_nr

## Location
src/interfaces/ecpg/ecpglib/connect.c: 36 - 75

## Overview
Retrieves a database connection structure by name, handling both named connections and the special current/default connection cases in a thread-safe manner.

## Definition
```c
static struct connection *ecpg_get_connection_nr(const char *connection_name)
```

## Detailed Description
This function implements the core connection lookup logic for the ECPG library. It handles two main scenarios: retrieving the current/default connection and retrieving named connections. For current connections (when connection_name is NULL or "CURRENT"), it first ensures pthread infrastructure is initialized, then attempts to get the thread-specific connection. If no thread-specific connection exists, it falls back to the global default connection. For named connections, it searches through the linked list of all active connections to find a matching name.

## Parameters / Member Variables
- `connection_name`: The name of the connection to retrieve. Special values:
  - `NULL`: Returns the current connection for this thread
  - `"CURRENT"`: Same as NULL, returns the current connection
  - Any other string: Searches for a named connection with that exact name

## Dependencies
- Functions called/Symbols referenced:
  - `ecpg_pthreads_init` (ensures thread infrastructure is ready)
  - `pthread_getspecific` (retrieves thread-specific connection)
  - `strcmp` (string comparison for connection names)
- Called from (representative examples):
  - `ecpg_get_connection` (public connection retrieval interface)
  - `ECPGdisconnect` (when disconnecting specific connections)

## Notes and Other Information
- This is a static function, only accessible within connect.c
- Returns NULL if the requested connection is not found
- For current connections, implements a fallback mechanism: thread-specific → global default
- Handles the case where database connections were created with NULL names
- Thread-safe for retrieving current connections via pthread thread-specific data
- The returned connection structure contains the actual PostgreSQL connection (PGconn), autocommit status, and other connection metadata
- Part of ECPG's connection management infrastructure that supports both single-threaded and multi-threaded applications