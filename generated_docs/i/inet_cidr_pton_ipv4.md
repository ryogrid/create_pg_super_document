# inet_cidr_pton_ipv4

## Location
src/backend/utils/adt/inet_net_pton.c: 97 - 259

## Overview
Converts IPv4 network numbers from presentation format to network format, supporting hexadecimal, decimal octets, and CIDR notation with automatic classful network inference.

## Definition


## Detailed Description
This function parses IPv4 network addresses from string format into binary network format. It supports multiple input formats including hexadecimal notation (0x prefix), decimal dotted notation (192.168.1.0), and CIDR specifications (/24 suffix). When no CIDR specification is provided, the function automatically infers the network width based on classful networking rules (Class A, B, C, D, E).

The function handles hexadecimal input by consuming nybble strings and decimal input by parsing dotted decimal notation. It validates that decimal octets don't exceed 255 and ensures proper format compliance. For CIDR specifications, it parses the /prefix notation and validates that the prefix length doesn't exceed 32 bits.

The network byte order is assumed throughout the conversion process, meaning that network addresses like 192.5.5.240/28 will have the binary pattern 0b11110000 in the fourth octet.

## Parameters / Member Variables
- : Source string containing the IPv4 network address in presentation format
- : Destination buffer to store the converted binary network address
- : Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - EMSGSIZE (error constant for message size errors)
  - Standard C library functions: isxdigit, isupper, tolower, strchr, isdigit
- Called from (representative examples):
  - [pg_inet_net_pton](../p/pg_inet_net_pton.md) (src/backend/utils/adt/inet_net_pton.c:69)

## Notes and Other Information
- Returns the number of bits in the network specification or -1 on failure
- Supports automatic classful network inference when no CIDR is specified:
  - Class A (0-127): 8 bits default
  - Class B (128-191): 16 bits default  
  - Class C (192-223): 24 bits default
  - Class D (224-239): 8 bits default (4 bits for 224.0.0.0 exactly)
  - Class E (240-255): 32 bits default
- Error handling sets errno to ENOENT for invalid specifications or EMSGSIZE for buffer overflow
- Hexadecimal format supports both uppercase and lowercase digits
- The function extends the network representation to cover the actual mask if needed
- Validates that CIDR prefix length doesn't exceed 32 bits for IPv4