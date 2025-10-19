# inet_net_pton_ipv6

## Location
[src/backend/utils/adt/inet_net_pton.c:429-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L429-L433)

## Overview
A wrapper function that converts IPv6 network numbers from presentation format to network format, specifically designed for handling networks without size constraints.

## Definition

```c
static int
inet_net_pton_ipv6(const char *src, u_char *dst)
```
## Detailed Description
This function serves as a simplified interface to  for IPv6 network address conversion. It acts as a wrapper that calls  with a fixed size parameter of 16 bytes (NS_IN6ADDRSZ), which is the standard size for IPv6 addresses. This function is used when size validation is not required and the caller expects to work with full IPv6 addresses.

The function delegates all the actual parsing work to , providing a simpler interface for cases where the destination buffer size is known to be adequate for IPv6 addresses.

## Parameters / Member Variables
- `*src`: Input string containing the IPv6 network address in presentation format (human-readable form)
- `*dst`: Output buffer where the converted network address will be stored in binary network format
## Dependencies
- Functions called/Symbols referenced:
  - [inet_cidr_pton_ipv6](inet_cidr_pton_ipv6.md)
  - NS_IN6ADDRSZ (constant: 16)
- Called from (representative examples):
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md)

## Notes and Other Information
- This is a static function internal to the inet_net_pton.c file
- Returns the number of bits in the network mask, or -1 on error
- The function assumes the destination buffer is large enough to hold a full IPv6 address (16 bytes)
- Part of PostgreSQL's network address handling infrastructure
- Originally derived from ISC (Internet Systems Consortium) code
- Used specifically when size constraints don't need to be validated (size parameter is -1 in the caller)

## Simplified Source

```c
static int
inet_net_pton_ipv6(const char *src, u_char *dst)
{
    // Simple wrapper that delegates to inet_cidr_pton_ipv6
    // with fixed 16-byte size for IPv6 addresses
    return inet_cidr_pton_ipv6(src, dst, 16);
}
```