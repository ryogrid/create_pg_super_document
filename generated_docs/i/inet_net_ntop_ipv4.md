# inet_net_ntop_ipv4

## Location
[src/port/inet_net_ntop.c:114-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/inet_net_ntop.c#L114-L154)

## Overview
Converts IPv4 network addresses from binary network format to presentation format with CIDR notation, formatting all four octets regardless of mask length.

## Definition

```c
static char *
inet_net_ntop_ipv4(const u_char *src, int bits, char *dst, size_t size)
```
## Detailed Description
This static function handles the IPv4-specific conversion from network byte order binary format to dotted-decimal notation with optional CIDR suffix. It always formats all four octets of the IPv4 address, regardless of the mask length specified. The function assumes network byte order input, meaning that for an address like 192.5.5.240/28, the fourth octet contains 0b11110000. The CIDR mask length is appended unless it's 32 bits (full address).

The function was authored by Paul Vixie (ISC) in October 1998 and serves as the IPv4 backend for PostgreSQL's network address presentation formatting.

## Parameters / Member Variables
- : Pointer to the 4-byte IPv4 address in network byte order
- : Number of network bits for CIDR notation (0-32)
- : Output buffer to store the formatted string
- : Size of the destination buffer

## Dependencies
- Functions called/Symbols referenced:
  - SPRINTF (macro for formatted string output)
  - EMSGSIZE (error constant for insufficient buffer size)
  - EINVAL (error constant for invalid input)
- Called from (representative examples):
  - [pg_inet_net_ntop](../p/pg_inet_net_ntop.md)

## Notes and Other Information
- Returns pointer to dst on success, NULL on error (check errno)
- Validates bits parameter range (0-32) and returns EINVAL for invalid values
- Always formats all four octets regardless of mask length
- Omits "/32" suffix for full addresses (32-bit masks)
- Returns EMSGSIZE error if output buffer is too small
- Assumes network byte order input format