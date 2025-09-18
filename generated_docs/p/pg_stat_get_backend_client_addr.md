# pg_stat_get_backend_client_addr

## Location
src/backend/utils/adt/pgstatfuncs.c: 879 - 923

## Overview
Returns the client IP address of a backend process identified by its process number, accessible through PostgreSQL's statistics views.

## Definition


## Detailed Description
This function retrieves the client IP address for a specific backend process in PostgreSQL. It takes a backend process number as input and returns the IP address of the client connected to that backend. The function performs several validation checks:

1. Validates that the backend entry exists
2. Checks user permissions to access the statistics information
3. Verifies that client address information is available (not zeroed)
4. Supports both IPv4 and IPv6 addresses
5. Converts the internal socket address to a human-readable IP address string

The function returns NULL if the backend doesn't exist, the user lacks permissions, no client address is available, or if address resolution fails.

## Parameters / Member Variables
-  (int32): The backend process number to query for client address information

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_beentry_by_proc_number
  - HAS_PGSTAT_PERMISSIONS
  - pg_getnameinfo_all
  - clean_ipv6_addr
  - inet_in
  - DirectFunctionCall1
  - CStringGetDatum
- Data types used:
  - PgBackendStatus
  - SockAddr

## Notes and Other Information
- The function is used internally by PostgreSQL's statistics system functions
- Returns NULL when client address is unavailable or access is denied
- Handles both IPv4 (AF_INET) and IPv6 (AF_INET6) address families
- Performs IPv6 address cleaning to ensure proper formatting
- Used by system views like pg_stat_activity to display client connection information
- Access is subject to PostgreSQL's statistics permissions model