# getHostaddr

## Location
[src/interfaces/libpq/fe-connect.c:2060-2092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2060-L2092)

## Overview
Extracts and formats the currently connected IP address from the connection socket into a string buffer.

## Definition

```c
struct sockaddr_storage *addr = &conn->raddr.addr;
```
## Detailed Description
This static function retrieves the IP address of the currently connected remote host from the connection's socket address structure and converts it to a human-readable string format. It handles both IPv4 and IPv6 addresses by examining the address family and using the appropriate conversion method. If the address cannot be converted or is of an unsupported family, the function sets the output string to empty.

The function operates on the remote address (raddr) stored in the PGconn structure, which contains the actual address of the connected PostgreSQL server. It uses pg_inet_net_ntop() to perform the network-to-presentation conversion, which is PostgreSQL's wrapper around the standard inet_ntop() function.

Key features:
- Supports both IPv4 (AF_INET) and IPv6 (AF_INET6) address families
- Gracefully handles conversion failures by setting empty string
- Uses appropriate bit lengths (32 for IPv4, 128 for IPv6)
- Ensures output buffer safety by respecting the provided length

## Parameters / Member Variables
- : Pointer to PGconn structure containing connection information and socket address
- : Output buffer to store the formatted IP address string
- : Length of the output buffer to prevent overflow

## Dependencies
- Functions called/Symbols referenced:
  - [pg_inet_net_ntop](../p/pg_inet_net_ntop.md) (for both IPv4 and IPv6 conversion)
- Called from (representative examples):
  - Connection handling code (referenced by CONNECTION_FAILED)

## Notes and Other Information
- The function is static (internal to fe-connect.c)
- Returns void; success/failure is indicated by empty vs non-empty output string
- Uses the sockaddr_storage structure to handle both IPv4 and IPv6 generically
- For IPv4 addresses, extracts sin_addr.s_addr from sockaddr_in structure
- For IPv6 addresses, extracts sin6_addr.s6_addr from sockaddr_in6 structure
- Sets output to empty string for unsupported address families or conversion failures
- The bit length parameters (32 for IPv4, 128 for IPv6) represent the full address width