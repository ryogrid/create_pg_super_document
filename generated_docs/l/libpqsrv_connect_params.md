# libpqsrv_connect_params

## Location
[src/include/libpq/libpq-be-fe-helpers.h:84-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L84-L106)

## Overview
A convenience wrapper around PQconnectdbParams() that handles file descriptor reservation and processes interrupts during connection establishment using parameter arrays.

## Definition
```c
static inline PGconn *libpqsrv_connect_params(const char *const *keywords,
                                               const char *const *values,
                                               int expand_dbname,
                                               uint32 wait_event_info)
```

## Detailed Description
libpqsrv_connect_params provides a server-side wrapper for PostgreSQL connection establishment using parameter arrays instead of a connection string. Like libpqsrv_connect, it handles resource management concerns specific to server processes by reserving file descriptors and ensuring proper interrupt processing during connection establishment. The function follows the same prepare-connect-finalize pattern by calling libpqsrv_connect_prepare(), then using PQconnectStartParams() for asynchronous connection initiation, and finally calling libpqsrv_connect_internal() to complete the connection process.

This variant is particularly useful when connection parameters are already structured as separate keyword-value pairs rather than being concatenated into a single connection string. The expand_dbname parameter controls whether the dbname parameter should be treated as a fallback connection string.

## Parameters / Member Variables
- `keywords`: Array of connection parameter keywords (null-terminated)
- `values`: Array of connection parameter values corresponding to keywords (null-terminated)
- `expand_dbname`: Boolean flag indicating whether to expand dbname as a connection string
- `wait_event_info`: Event identifier used for wait event reporting during connection establishment

## Dependencies
- Functions called/Symbols referenced:
  - [libpqsrv_connect_prepare](libpqsrv_connect_prepare.md)
  - [PQconnectStartParams](../P/PQconnectStartParams.md)
  - [libpqsrv_connect_internal](libpqsrv_connect_internal.md)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a static inline function defined in src/include/libpq/libpq-be-fe-helpers.h:84-106
- Similar to libpqsrv_connect but uses parameter arrays instead of connection strings
- Callers must use PQstatus() to verify if the returned connection is valid since connection failures do not result in thrown errors
- Part of the libpqsrv suite of functions designed for server-side PostgreSQL connection management
- The keywords and values arrays must have the same length and be null-terminated

## Simplified Source

```c
static inline PGconn *
libpqsrv_connect_params(const char *const *keywords,
                        const char *const *values,
                        int expand_dbname,
                        uint32 wait_event_info)
{
    PGconn *conn = NULL;

    // Reserve file descriptor and prepare for connection
    libpqsrv_connect_prepare();

    // Start asynchronous connection with parameter arrays
    conn = PQconnectStartParams(keywords, values, expand_dbname);

    // Complete connection with interrupt handling
    libpqsrv_connect_internal(conn, wait_event_info);

    return conn;
}
```

This function wraps PQconnectdbParams() to provide server-side connection management using parameter arrays instead of connection strings, with file descriptor reservation and interrupt processing.