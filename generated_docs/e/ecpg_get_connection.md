# ecpg_get_connection

## Location
src/interfaces/ecpg/ecpglib/connect.c: 76 - 107

## Overview
Retrieves a database connection object by name from the ECPG library's connection pool, supporting both thread-specific and globally shared connection management.

## Definition

```c
struct connection *ret = NULL;
```
## Detailed Description
The  function is a central connection retrieval mechanism in PostgreSQL's ECPG (Embedded SQL in C) library. It provides thread-safe access to database connections with support for both named connections and the current/default connection.

The function implements a two-tier connection lookup strategy:
1. **Current/Default Connection**: When  is NULL or "CURRENT", it first attempts to retrieve a thread-specific connection using pthread thread-specific data (TSD). If no thread-specific connection exists, it falls back to the global default connection.
2. **Named Connection**: For named connections, it uses mutex-protected access to search through the global connection list.

The function handles thread safety through two mechanisms:
- Thread-specific data for current connections (no mutex needed)
- Mutex protection () for named connection lookups

## Parameters
- : The name of the connection to retrieve. Special values:
  - : Retrieves the current connection (thread-specific or global default)
  - : Same as NULL, retrieves the current connection
  - Any other string: Searches for a named connection in the global connection list

## Dependencies
- Functions called/Symbols referenced:
  - : Ensures pthread key initialization for thread-specific storage
  - : Retrieves thread-specific connection data
  - : Locks the connections mutex for safe access to named connections
  - : Internal helper function to search for named connections
  - : Releases the connections mutex
- Called from (representative examples):
  - : Set autocommit mode for a connection
  - : Set the current connection
  - : Establish database connections
  - : Get the underlying libpq connection object
  - : Prepare for SQL statement execution
  - : Get connection status
  - : Prepare SQL statements
  - Various other ECPG API functions that need connection access

## Notes and Other Information
- **Thread Safety**: The function is thread-safe through careful use of pthread mechanisms. Current connections use thread-specific data without mutex protection, while named connections are protected by the .
- **Fallback Strategy**: For current connections, the function implements a fallback from thread-specific to global connection, allowing flexibility in multi-threaded applications.
- **Connection Structure**: Returns a pointer to a  which contains the connection name, libpq PGconn object, autocommit flag, prepared statement cache, and type information cache.
- **Error Handling**: Returns NULL if the requested connection cannot be found or accessed.
- **Usage Pattern**: This is primarily an internal function used by higher-level ECPG API functions rather than being called directly by user code.
- **Global Variables**: Relies on global variables like  (pthread key),  (global default), and  for coordination.