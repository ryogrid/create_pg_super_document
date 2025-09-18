# ECPGget_PGconn

## Location
src/interfaces/ecpg/ecpglib/connect.c: 722 - 731

## Overview
ECPGget_PGconn retrieves the underlying libpq PGconn connection object from a named ECPG connection, providing direct access to the PostgreSQL connection handle.

## Definition
```c
PGconn *ECPGget_PGconn(const char *connection_name)
```

## Detailed Description
ECPGget_PGconn is a utility function that provides access to the raw PostgreSQL connection handle (PGconn) that underlies an ECPG connection. This function allows applications to bypass the ECPG layer and directly use libpq functions when needed for advanced operations not supported by the embedded SQL interface. It performs a simple lookup of the named connection in the global connection list and returns the associated PGconn pointer, or NULL if the connection is not found.

## Parameters / Member Variables
- `connection_name`: Name of the ECPG connection for which to retrieve the underlying PGconn handle

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_get_connection
- Called from (representative examples):
  - Applications needing direct libpq access
  - Bridge code between ECPG and libpq interfaces

## Notes and Other Information
- Returns PGconn pointer on success, NULL if connection not found
- Provides a bridge between ECPGs high-level interface and libpqs low-level API
- Does not perform connection validation - relies on ecpg_get_connection for lookup
- The returned PGconn should not be closed directly; use ECPGdisconnect instead
- Useful for accessing libpq functions not available through ECPG embedded SQL
- Simple wrapper function with minimal overhead
- Thread-safe as it only performs read operations on the connection structure