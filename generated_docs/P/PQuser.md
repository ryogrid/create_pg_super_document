# PQuser

## Location
src/interfaces/libpq/fe-connect.c: 7011 - 7018

## Overview
PQuser returns the user name of the connection as a string pointer, providing access to the authenticated user identity associated with a PostgreSQL database connection.

## Definition
```c
char *PQuser(const PGconn *conn)
```

## Detailed Description
PQuser is a libpq client library function that retrieves the user name associated with an established PostgreSQL database connection. The function performs a simple null-check on the connection handle and returns a pointer to the user name string stored in the connection structure. This is a read-only accessor function that provides external access to the pguser field of the PGconn structure.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn connection object. If NULL, the function returns NULL safely.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function)
- Called from (representative examples):
  - exec_command_conninfo (src/bin/psql/command.c:689)
  - do_connect (src/bin/psql/command.c:3711)
  - SyncVariables (src/bin/psql/command.c:4053)
  - session_username (src/bin/psql/common.c:2163)

## Notes and Other Information
- Returns a pointer to the internal pguser string; the caller should not modify or free this string
- Returns NULL if the connection handle is NULL
- The returned string is valid for the lifetime of the connection object
- This function is part of the libpq public API for PostgreSQL client applications