# pg_inet_net_ntop

## Location
[src/port/inet_net_ntop.c:77-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/inet_net_ntop.c#L77-L113)

## Overview
Converts a network address from binary network format to presentation (string) format, supporting both IPv4 and IPv6 address families with CIDR notation.

## Definition


## Detailed Description
This function serves as a wrapper that delegates network-to-presentation conversion to appropriate address family-specific handlers. It handles both PostgreSQL-specific address family constants (PGSQL_AF_INET, PGSQL_AF_INET6) and system library constants (AF_INET, AF_INET6). The function can convert network addresses that include host parts, making it suitable for both network addresses and host addresses with netmasks.

The function was authored by Paul Vixie (ISC) in October 1998 and is part of PostgreSQL's portable network address handling infrastructure.

## Parameters / Member Variables
- : Address family constant (PGSQL_AF_INET, PGSQL_AF_INET6, AF_INET6)
- : Pointer to the binary network address to convert
- : Number of network bits (for CIDR notation)
- : Output buffer to store the presentation format string
- : Size of the destination buffer

## Dependencies
- Functions called/Symbols referenced:
  - [inet_net_ntop_ipv4](../i/inet_net_ntop_ipv4.md)
  - [inet_net_ntop_ipv6](../i/inet_net_ntop_ipv6.md)
  - PGSQL_AF_INET
  - PGSQL_AF_INET6
  - EAFNOSUPPORT
- Called from (representative examples):
  - [network_out](../n/network_out.md)
  - [network_host](../n/network_host.md)
  - [network_show](../n/network_show.md)
  - [inet_abbrev](../i/inet_abbrev.md)
  - getHostaddr
  - [pq_verify_peer_name_matches_certificate_ip](pq_verify_peer_name_matches_certificate_ip.md)

## Notes and Other Information
- Returns pointer to dst on success, or NULL on error (check errno)
- Can handle addresses like 192.5.5.1/28 which have nonzero host parts
- Safely handles both PostgreSQL and system address family constants
- Part of PostgreSQL's portable network utility functions in src/port/