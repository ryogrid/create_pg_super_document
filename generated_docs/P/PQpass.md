# PQpass

## Location
[src/interfaces/libpq/fe-connect.c:7019-7035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7019-L7035)

## Overview
PQpass returns the password associated with a PostgreSQL database connection, implementing a priority-based lookup across multiple password sources.

## Definition
```c
char *PQpass(const PGconn *conn)
```

## Detailed Description
PQpass is a libpq client library function that retrieves the password associated with an established PostgreSQL database connection. The function implements a hierarchical password lookup strategy: first checking the host-specific password in the connhost array for the current host, then falling back to the global pgpass field, and finally returning an empty string if no password is found. This design supports multi-host connection configurations where different hosts may have different passwords.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn connection object. If NULL, the function returns NULL safely.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function with conditional logic)
- Called from (representative examples):
  - [ConnectDatabase](../C/ConnectDatabase.md) (src/bin/pg_dump/pg_backup_db.c:208)
  - [PQconnectionNeedsPassword](PQconnectionNeedsPassword.md) (src/interfaces/libpq/fe-connect.c:7216)

## Notes and Other Information
- Returns a pointer to the password string; the caller should not modify or free this string
- Returns NULL if the connection handle is NULL
- Returns an empty string ("") rather than NULL when no password is specified, maintaining historical compatibility
- Supports multi-host configurations by checking host-specific passwords first
- The returned string is valid for the lifetime of the connection object
- Part of the libpq public API for PostgreSQL client applications