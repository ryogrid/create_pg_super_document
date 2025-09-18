# PQhostaddr

## Location
src/interfaces/libpq/fe-connect.c: 7059 - 7071

## Overview
PQhostaddr returns the parsed IP address of the current PostgreSQL database connection, providing access to the resolved network address actually used for the connection.

## Definition
```c
char *PQhostaddr(const PGconn *conn)
```

## Detailed Description
PQhostaddr is a libpq client library function that retrieves the parsed IP address associated with an established PostgreSQL database connection. Unlike PQhost which returns the original host specification, this function returns the actual resolved IP address stored in the connip field after hostname resolution and connection establishment. This provides applications with the concrete network address that was used for the database connection, which is particularly useful for logging, diagnostics, and network troubleshooting.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn connection object. If NULL, the function returns NULL safely.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function)
- Called from (representative examples):
  - exec_command_conninfo (src/bin/psql/command.c:682)
  - do_connect (src/bin/psql/command.c:3805)

## Notes and Other Information
- Returns a pointer to the parsed IP address string; the caller should not modify or free this string
- Returns NULL if the connection handle is NULL
- Returns an empty string ("") when no IP address information is available
- Provides the actual resolved IP address, not the original host specification
- Requires both connhost and connip to be available to return a valid address
- The returned string is valid for the lifetime of the connection object
- Part of the libpq public API for PostgreSQL client applications
- Useful for applications that need to know the actual network endpoint used for the connection