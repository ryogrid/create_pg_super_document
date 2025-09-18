# inet_cidr_ntop_ipv4

## Location
src/backend/utils/adt/inet_cidr_ntop.c: 85 - 164

## Overview
Converts IPv4 network addresses from binary format to CIDR presentation format, handling partial octet formatting based on network prefix length.

## Definition
```c
static char *inet_cidr_ntop_ipv4(const u_char *src, int bits, char *dst, size_t size)
```

## Detailed Description
This static function performs the actual conversion of IPv4 network addresses from their binary representation to human-readable CIDR format. It intelligently formats only the significant portions of the address based on the network prefix length, including partial octets when necessary.

The function handles three main formatting scenarios: complete octets (for bits divisible by 8), partial octets (for remaining bits), and the CIDR prefix notation. It applies network masking to ensure only network-significant bits are displayed, making it suitable for representing network ranges rather than individual host addresses.

The algorithm processes octets sequentially, formatting complete 8-bit groups first, then handling any remaining partial octet with appropriate bit masking. All output includes the CIDR /prefix notation.

## Parameters / Member Variables
- `src`: Pointer to the source IPv4 address in network byte order (4 bytes)
- `bits`: Number of network prefix bits (0-32), determines how many bits to format
- `dst`: Destination buffer to store the formatted CIDR string
- `size`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - SPRINTF (macro for safe string formatting)
  - EINVAL (error code for invalid input parameters)
  - EMSGSIZE (error code for insufficient buffer space)
- Called from (representative examples):
  - pg_inet_cidr_ntop (when processing IPv4 addresses)

## Notes and Other Information
- Returns pointer to destination buffer on success, NULL on error with errno set
- Input validation ensures bits parameter is within valid range (0-32)
- Special handling for zero-length networks (bits=0) outputs just "0"
- Network byte order assumed for input address
- Partial octet masking uses bit shifting: `((1 << b) - 1) << (8 - b)`
- Buffer size checking prevents overflow with detailed size calculations
- Originally authored by Paul Vixie (ISC) in July 1996
- Function is static (internal linkage) within the inet_cidr_ntop.c module