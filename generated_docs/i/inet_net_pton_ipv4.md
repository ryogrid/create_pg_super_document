# inet_net_pton_ipv4

## Location
src/backend/utils/adt/inet_net_pton.c: 260 - 348

## Overview
Converts IPv4 network addresses from presentation format to network format, accepting standard dotted decimal notation with optional CIDR suffix and allowing host addresses with netmasks.

## Definition


## Detailed Description
This function parses IPv4 addresses from string format into binary format, accepting standard dotted decimal notation with optional CIDR prefix specification. Unlike inet_cidr_pton_ipv4, this function is designed to handle host addresses with included netmasks, meaning it accepts addresses like 192.5.5.1/28 which have nonzero host parts.

The function parses decimal octets separated by dots, validates that each octet value doesn't exceed 255, and handles optional CIDR prefix notation. It requires that all four octets be specified if no CIDR prefix is given (defaulting to /32). The function extends shorter addresses to four full octets by padding with zeros.

Key differences from inet_cidr_pton_ipv4: this function doesn't perform classful network inference and requires explicit specification of all octets for non-CIDR addresses. It's designed for parsing complete IPv4 addresses rather than network specifications.

## Parameters / Member Variables
- : Source string containing the IPv4 address in dotted decimal notation with optional CIDR suffix
- : Destination buffer to store the converted binary address (assumed to be large enough for IPv4)

## Dependencies
- Functions called/Symbols referenced:
  - EMSGSIZE (error constant for message size errors)
  - ENOENT (error constant for invalid format)
  - Standard C library functions: isdigit, strchr
- Called from (representative examples):
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md) (src/backend/utils/adt/inet_net_pton.c:68)

## Notes and Other Information
- Returns the number of bits in the prefix length on success, -1 on failure
- Defaults to /32 prefix length only when all four octets are specified
- Validates that CIDR prefix length doesn't exceed 32 bits for IPv4
- Validates that prefix length doesn't overspecify the mantissa (e.g., /25 requires at least 4 octets)
- Error handling sets errno to ENOENT for invalid format or EMSGSIZE for size errors
- Always extends the address representation to four full octets
- Accepts host addresses with netmasks, unlike pure network address parsers
- Authored by Paul Vixie (ISC) in October 1998