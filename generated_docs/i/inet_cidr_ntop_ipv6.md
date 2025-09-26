# inet_cidr_ntop_ipv6

## Location
[src/backend/utils/adt/inet_cidr_ntop.c:165-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_cidr_ntop.c#L165-L294)

## Overview
Converts IPv6 network addresses from binary format to CIDR presentation format with zero compression optimization and IPv4-mapped address detection.

## Definition
```c
static char *inet_cidr_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)
```

## Detailed Description
This static function handles the complex conversion of IPv6 network addresses from their 16-byte binary representation to human-readable CIDR format. It implements sophisticated formatting logic including zero compression (finding and collapsing the longest sequence of zero fields), IPv4-mapped address detection, and intelligent prefix-based truncation.

The function operates in several phases: input validation, network masking based on prefix length, zero sequence detection and compression, special IPv4-mapped address handling, and final CIDR notation formatting. It uses an internal buffer to build the output safely before copying to the destination.

Key algorithmic features include finding the longest consecutive sequence of zero 16-bit words for compression (:: notation), detecting IPv4-mapped IPv6 addresses for mixed notation output, and handling partial word formatting when the prefix length doesn't align with 16-bit boundaries.

## Parameters / Member Variables
- `src`: Pointer to the source IPv6 address in network byte order (16 bytes)  
- `bits`: Number of network prefix bits (0-128), determines formatting scope
- `dst`: Destination buffer to store the formatted CIDR string
- `size`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - SPRINTF (macro for safe string formatting)
  - memcpy (memory copy for address buffer)
  - memset (memory initialization for zero padding)
  - strlen (string length calculation)
  - strcpy (final string copy to destination)
  - EINVAL (error code for invalid bit count)
  - EMSGSIZE (error code for insufficient buffer space)
- Called from (representative examples):
  - [pg_inet_cidr_ntop](../p/pg_inet_cidr_ntop.md) (when processing IPv6 addresses)

## Notes and Other Information
- Returns pointer to destination buffer on success, NULL on error with errno set
- Input validation ensures bits parameter is within valid range (0-128)  
- Uses internal 16-byte buffer for network masking and output formatting buffer
- Special case handling for zero-length networks (bits=0) outputs "::"
- Zero compression algorithm finds longest consecutive zero sequence for :: notation
- IPv4-mapped address detection for addresses like ::ffff:192.0.2.1
- [Complex](../C/Complex.md) logic handles various IPv6 formatting edge cases and optimizations
- Network byte order assumed for all input processing
- Originally based on Paul Vixie's IPv4 version, adapted by Vadim Kogan (UCB) in June 2001
- Function is static (internal linkage) within the inet_cidr_ntop.c module
- Output buffer size requirement can be up to 49 characters for full IPv6 CIDR notation