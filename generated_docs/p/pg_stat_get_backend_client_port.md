# pg_stat_get_backend_client_port

## Location
src/backend/utils/adt/pgstatfuncs.c: 924 - 969

## Overview
Returns the client port number of a backend process identified by its process number, accessible through PostgreSQL's statistics views.

## Definition
```c
Datum pg_stat_get_backend_client_port(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the client port number for a specific backend process in PostgreSQL. It takes a backend process number as input and returns the port number of the client connected to that backend. The function performs several validation checks similar to its address counterpart:

1. Validates that the backend entry exists
2. Checks user permissions to access the statistics information
3. Verifies that client address information is available (not zeroed)
4. Supports IPv4, IPv6, and Unix socket connections
5. For Unix sockets (AF_UNIX), returns -1 as a special indicator
6. Converts the internal socket port to an integer value

The function returns NULL if the backend doesn't exist, the user lacks permissions, no client address is available, or if port resolution fails.

## Parameters / Member Variables
- `procNumber` (int32): The backend process number to query for client port information

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md)
  - HAS_PGSTAT_PERMISSIONS
  - pg_getnameinfo_all
  - [int4in](../i/int4in.md)
  - DirectFunctionCall1
  - [CStringGetDatum](../C/CStringGetDatum.md)
- Data types used:
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - [SockAddr](../S/SockAddr.md)

## Notes and Other Information
- The function is used internally by PostgreSQL's statistics system functions
- Returns NULL when client port is unavailable or access is denied
- Handles IPv4 (AF_INET), IPv6 (AF_INET6), and Unix socket (AF_UNIX) connections
- Unix socket connections return -1 since they don't have meaningful port numbers
- Used by system views like pg_stat_activity to display client connection information
- Access is subject to PostgreSQL's statistics permissions model
- Complements pg_stat_get_backend_client_addr for complete client connection information