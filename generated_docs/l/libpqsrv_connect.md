# libpqsrv_connect

## Location
[src/include/libpq/libpq-be-fe-helpers.h:66-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L66-L83)

## Overview
A convenience wrapper around PQconnectdb() that handles file descriptor reservation and processes interrupts during connection establishment to PostgreSQL databases.

## Definition

```c
static inline PGconn *
libpqsrv_connect(const char *conninfo, uint32 wait_event_info)
```
## Detailed Description
libpqsrv_connect provides a server-side wrapper for PostgreSQL connection establishment that addresses resource management concerns specific to server processes. It reserves a file descriptor before attempting connection and ensures that interrupts are properly processed during the connection establishment phase. The function follows a prepare-connect-finalize pattern by first calling libpqsrv_connect_prepare(), then using PQconnectStart() for asynchronous connection initiation, and finally calling libpqsrv_connect_internal() to complete the connection process with interrupt handling.

The function will throw an error if file descriptor acquisition fails through AcquireExternalFD(), but does not throw errors for connection establishment failures themselves. This design allows callers to distinguish between resource exhaustion issues and network/authentication problems.

## Parameters / Member Variables
- `conninfo`: Connection string in PostgreSQL format containing database connection parameters
- `wait_event_info`: Event identifier used for wait event reporting during connection establishment

## Dependencies
- Functions called/Symbols referenced:
  - [libpqsrv_connect_prepare](libpqsrv_connect_prepare.md)
  - [PQconnectStart](../P/PQconnectStart.md)
  - [libpqsrv_connect_internal](libpqsrv_connect_internal.md)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a static inline function defined in src/include/libpq/libpq-be-fe-helpers.h:66-83
- Callers must use PQstatus() to verify if the returned connection is valid since connection failures do not result in thrown errors
- Part of the libpqsrv suite of functions designed for server-side PostgreSQL connection management
- The function handles asynchronous connection establishment which allows for proper interrupt processing during potentially long connection setup phases

## Simplified Source

```c
static inline PGconn *
libpqsrv_connect(const char *conninfo, uint32 wait_event_info)
{
    PGconn *conn = NULL;

    // Reserve file descriptor and prepare for connection
    libpqsrv_connect_prepare();

    // Start asynchronous connection
    conn = PQconnectStart(conninfo);

    // Complete connection with interrupt handling
    libpqsrv_connect_internal(conn, wait_event_info);

    return conn;
}
```

This function wraps PQconnectdb() to provide server-side connection management, including file descriptor reservation and interrupt processing during connection establishment.