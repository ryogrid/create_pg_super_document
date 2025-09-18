# inet_server_addr

## Location
src/backend/utils/adt/network.c: 1788 - 1824

## Overview
Returns the IP address that the PostgreSQL server accepted the current connection on, or NULL if the connection is through a Unix socket.

## Definition


## Detailed Description
This function retrieves the local IP address of the server socket that accepted the current client connection. It accesses the connection information stored in MyProcPort and extracts the local address (laddr) from the port structure. The function supports both IPv4 (AF_INET) and IPv6 (AF_INET6) address families and returns NULL for Unix domain sockets or when address resolution fails.

The function uses pg_getnameinfo_all() to convert the binary socket address to a string representation in numeric format, then cleans up IPv6 addresses if necessary using clean_ipv6_addr(), and finally converts the string to PostgreSQL's internal inet type using network_in().

## Parameters / Member Variables
This function takes no explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - Port (struct type from MyProcPort)
  - pg_getnameinfo_all (address-to-string conversion)
  - clean_ipv6_addr (IPv6 address formatting)
  - network_in (string to inet type conversion)
  - PG_RETURN_INET_P (PostgreSQL return macro)
- Called from (representative examples):
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- Returns NULL for Unix socket connections (non-TCP connections)
- Only supports IPv4 and IPv6 address families
- Uses NI_NUMERICHOST | NI_NUMERICSERV flags to ensure numeric output format
- Part of PostgreSQL's network data type functions
- Accessible from SQL as inet_server_addr() function
- Relies on MyProcPort global variable which contains current connection information