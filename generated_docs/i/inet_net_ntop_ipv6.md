# inet_net_ntop_ipv6

## Location
[src/port/inet_net_ntop.c:178-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/inet_net_ntop.c#L178-L296)

## Overview
Converts IPv6 network addresses from binary network format to presentation format with CIDR notation, implementing RFC-compliant IPv6 address compression and embedded IPv4 handling.

## Definition


## Detailed Description
This static function handles IPv6-specific conversion from 16-byte binary format to standard IPv6 text representation. It implements several key IPv6 formatting features: zero compression (:: notation) for the longest run of consecutive zero 16-bit groups, detection and formatting of embedded IPv4 addresses (IPv4-mapped, IPv4-compatible, and 6to4 addresses), and optional CIDR prefix notation. The function uses a two-pass algorithm: first preprocessing to find optimal zero compression opportunities, then formatting the final output string.

The implementation is portable across different architectures, avoiding assumptions about integer sizes and byte ordering that might not hold on all systems.

## Parameters / Member Variables
- : Pointer to the 16-byte IPv6 address in network byte order
- : Number of network bits for CIDR notation (-1 for no prefix, 0-128)
- : Output buffer to store the formatted string
- : Size of the destination buffer

## Dependencies
- Functions called/Symbols referenced:
  - [decoct](../d/decoct.md) (for IPv4 embedded address formatting)
  - SPRINTF (macro for formatted string output)
  - NS_IN6ADDRSZ (IPv6 address size constant)
  - NS_INT16SZ (16-bit integer size constant)
  - EMSGSIZE (error constant for insufficient buffer size)
  - EINVAL (error constant for invalid input)
- Called from (representative examples):
  - [pg_inet_net_ntop](../p/pg_inet_net_ntop.md)

## Notes and Other Information
- Returns pointer to dst on success, NULL on error (check errno)
- Validates bits parameter range (-1 to 128) and returns EINVAL for invalid values
- Implements RFC-compliant zero compression with :: notation
- Detects and formats embedded IPv4 addresses in various IPv6 formats
- Omits CIDR prefix for -1 bits parameter or when bits equals 128
- Uses temporary buffer to build output, then copies to destination
- Returns EMSGSIZE error if output buffer is too small
- Portable implementation that works across different architectures