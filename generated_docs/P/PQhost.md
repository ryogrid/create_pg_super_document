# PQhost

## Location
src/interfaces/libpq/fe-connect.c: 7036 - 7058

## Overview
PQhost returns the host name or address associated with a PostgreSQL database connection, implementing a fallback strategy from hostname to host address.

## Definition
```c
char *PQhost(const PGconn *conn)
```

## Detailed Description
PQhost is a libpq client library function that retrieves the host identifier associated with an established PostgreSQL database connection. The function implements a priority-based lookup: it first returns the verbatim host value provided by the user if available and non-empty, then falls back to the hostaddr value if the host is empty or unavailable, and finally returns an empty string if neither is available. This design supports both hostname and IP address specifications while preserving the original user input when possible.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn connection object. If NULL, the function returns NULL safely.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function with conditional logic)
- Called from (representative examples):
  - libpqrcv_get_senderinfo (src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:430)
  - main (src/bin/pgbench/pgbench.c:7249)
  - exec_command_conninfo (src/bin/psql/command.c:681)
  - do_connect (src/bin/psql/command.c:3801)
  - pg_GSS_load_servicename (src/interfaces/libpq/fe-gssapi-common.c:94)

## Notes and Other Information
- Returns a pointer to the host string; the caller should not modify or free this string
- Returns NULL if the connection handle is NULL
- Returns an empty string ("") when no host information is available
- Prioritizes the original host value over hostaddr to preserve user intent
- Supports multi-host connection configurations by checking the current active host (whichhost)
- The returned string is valid for the lifetime of the connection object
- Part of the libpq public API for PostgreSQL client applications