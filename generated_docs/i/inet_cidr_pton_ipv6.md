# inet_cidr_pton_ipv6

## Location
[src/backend/utils/adt/inet_net_pton.c:439-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L439-L564)

## Overview
Converts IPv6 network addresses from presentation format to network format, handling CIDR notation, IPv4-mapped addresses, and size constraints.

## Definition


## Detailed Description
This function is the core IPv6 address parser that converts IPv6 network addresses from human-readable presentation format into binary network format. It supports full IPv6 syntax including:

- Standard IPv6 colon-separated hexadecimal notation
- Compressed notation with "::", representing zero compression
- IPv4-mapped IPv6 addresses (e.g., ::ffff:192.168.1.1)
- CIDR prefix notation (e.g., 2001:db8::/32)
- Mixed case hexadecimal digits

The function performs comprehensive validation of the input format and handles various edge cases in IPv6 address parsing. It uses a finite state machine approach to parse the address character by character, building the binary representation while tracking state such as zero compression location and handling transitions between different address components.

The parser supports the special "::" notation for zero compression, which can appear anywhere in the address to represent one or more groups of zeros. It also handles IPv4 dotted-decimal notation when it appears at the end of an IPv6 address for IPv4-mapped or IPv4-compatible addresses.

## Parameters / Member Variables
- : Input string containing the IPv6 network address in presentation format
- : Output buffer where the converted network address will be stored in binary format
- : Size of the destination buffer in bytes (must be at least NS_IN6ADDRSZ = 16 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - strchr (standard C library)
  - memset (standard C library) 
  - memcpy (standard C library)
  - [getv4](../g/getv4.md) (for parsing IPv4-mapped addresses)
  - [getbits](../g/getbits.md) (for parsing CIDR prefix notation)
- Constants referenced:
  - NS_IN6ADDRSZ (16 - size of IPv6 address)
  - NS_INT16SZ (2 - size of 16-bit integer)
  - NS_INADDRSZ (4 - size of IPv4 address)
  - EMSGSIZE (errno value for insufficient buffer size)
  - ENOENT (errno value for invalid input)
- Called from (representative examples):
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md)
  - [inet_net_pton_ipv6](inet_net_pton_ipv6.md)

## Notes and Other Information
- Returns the number of network bits specified by CIDR notation, or 128 if no prefix is specified
- Returns -1 on error with errno set appropriately (ENOENT for invalid format, EMSGSIZE for insufficient buffer)
- Handles the complex IPv6 zero compression (::) by using a two-pass approach: first parse into a temporary buffer, then rearrange if zero compression was used
- The function is careful to validate buffer boundaries to prevent overflow
- Supports both lowercase and uppercase hexadecimal digits
- Originally based on code from Internet Systems Consortium (ISC)
- This is a static function internal to the inet_net_pton.c file
- The parsing algorithm handles the ambiguity of :: placement by tracking the colonp pointer to mark where zero compression should be applied