# getv4

## Location
[src/backend/utils/adt/inet_net_pton.c:382-428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L382-L428)

## Overview
Parses IPv4 address components from a string in dotted decimal notation, supporting both partial addresses and CIDR prefix specifications.

## Definition


## Detailed Description
This function parses IPv4 address components from a string containing dotted decimal notation. It processes decimal octets separated by dots and can handle partial IPv4 addresses (fewer than 4 octets). When a forward slash is encountered, it delegates CIDR prefix parsing to the getbits function.

The function validates that each decimal component doesn't exceed 255 (valid octet range) and enforces that no more than 4 octets are specified. It also prevents leading zeros in decimal numbers to avoid ambiguous interpretation. The function is primarily used as a helper in IPv6 address parsing where IPv4-mapped addresses or IPv4-compatible IPv6 addresses need to be handled.

The parsing stops when a slash is encountered (indicating CIDR notation) or when the string ends. For CIDR notation, it calls getbits to parse the prefix length.

## Parameters / Member Variables
- : Source string containing the IPv4 address in dotted decimal notation
- : Destination buffer to store the parsed octets
- : Pointer to integer where CIDR prefix length will be stored (if encountered)

## Dependencies
- Functions called/Symbols referenced:
  - [getbits](getbits.md) (for parsing CIDR prefix lengths)
  - Standard C library function: strchr
- Called from (representative examples):
  - [inet_cidr_pton_ipv6](../i/inet_cidr_pton_ipv6.md) (src/backend/utils/adt/inet_net_pton.c:507)

## Notes and Other Information
- Returns 1 on successful parsing, 0 on failure
- Validates that octets are in the range 0-255
- Prohibits leading zeros in decimal numbers
- Supports partial IPv4 addresses (1-4 octets)
- Handles CIDR notation by delegating to getbits when '/' is encountered
- Used primarily in IPv6 address parsing for handling embedded IPv4 addresses
- Enforces maximum of 4 octets to prevent buffer overflow
- The function maintains state tracking to prevent malformed input