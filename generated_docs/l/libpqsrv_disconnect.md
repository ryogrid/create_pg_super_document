# libpqsrv_disconnect

## Location
[src/include/libpq/libpq-be-fe-helpers.h:107-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L107-L130)

## Overview
A wrapper around PQfinish() that additionally releases the file descriptor reserved during connection establishment.

## Definition
```c
static inline void libpqsrv_disconnect(PGconn *conn)
```

## Detailed Description
libpqsrv_disconnect provides a clean teardown counterpart to the libpqsrv_connect family of functions. It handles both the PostgreSQL connection cleanup via PQfinish() and the file descriptor resource management by calling ReleaseExternalFD(). The function includes null-safety by allowing NULL connections to be passed, which simplifies error handling in calling code.

The design philosophy follows the principle that if a connection was never successfully established (conn == NULL), then no file descriptor was reserved for it, or it was already released during the failed connection attempt. This makes it easier to write exception handlers that can safely call libpqsrv_disconnect regardless of whether connection establishment succeeded.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle to disconnect and clean up, or NULL

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseExternalFD
  - [PQfinish](../P/PQfinish.md)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a static inline function defined in src/include/libpq/libpq-be-fe-helpers.h:107-130
- Safe to call with NULL connections, making it suitable for use in error handling paths
- Must be used as the counterpart to libpqsrv_connect or libpqsrv_connect_params to properly release reserved file descriptors
- Part of the resource management strategy for server-side PostgreSQL connections
- The function design simplifies PG_CATCH() exception handlers by allowing unconditional cleanup calls