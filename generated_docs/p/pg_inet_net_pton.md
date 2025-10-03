# pg_inet_net_pton

## Location
[src/backend/utils/adt/inet_net_pton.c:62-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L62-L96)

## Overview
Converts network numbers from presentation format to network format, supporting both IPv4 and IPv6 addresses with CIDR notation.

## Definition

```c
int
pg_inet_net_pton(int af, const char *src, void *dst, size_t size)
```
## Detailed Description
This function serves as a dispatcher that converts network addresses from human-readable string format to binary network format. It accepts various input formats including hexadecimal octets, hexadecimal strings, decimal octets, and CIDR notation (with /CIDR suffix). The function automatically detects the address family and delegates to the appropriate IPv4 or IPv6 conversion function based on the address family parameter and size argument.

The function was originally authored by Paul Vixie (ISC) in June 1996 and has been adapted for PostgreSQL use. It handles both network address parsing (when size is -1) and CIDR block parsing (when size is specified).

## Parameters / Member Variables
- `af`: Address family specification (PGSQL_AF_INET for IPv4, PGSQL_AF_INET6 for IPv6)
- `*src`: Source string containing the network address in presentation format
- `*dst`: Destination buffer to store the converted binary network address
- `size`: Size of the destination buffer in bytes, or -1 for network address parsing
## Dependencies
- Functions called/Symbols referenced:
  - [inet_net_pton_ipv4](../i/inet_net_pton_ipv4.md) (for IPv4 network parsing)
  - [inet_cidr_pton_ipv4](../i/inet_cidr_pton_ipv4.md) (for IPv4 CIDR parsing)
  - [inet_net_pton_ipv6](../i/inet_net_pton_ipv6.md) (for IPv6 network parsing)
  - [inet_cidr_pton_ipv6](../i/inet_cidr_pton_ipv6.md) (for IPv6 CIDR parsing)
  - PGSQL_AF_INET (IPv4 address family constant)
  - PGSQL_AF_INET6 (IPv6 address family constant)
  - EAFNOSUPPORT (error constant for unsupported address family)
- Called from (representative examples):
  - [network_in](../n/network_in.md) (src/backend/utils/adt/network.c:93)

## Notes and Other Information
- Returns the number of bits in the network specification (either inferred classfully or specified with CIDR notation)
- Returns -1 on failure with errno set appropriately (ENOENT indicates invalid network specification)
- The size parameter determines the parsing mode: -1 for network address parsing, positive value for CIDR block parsing
- Supports both IPv4 and IPv6 address families through delegation to specialized parsing functions