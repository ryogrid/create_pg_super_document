# inet_aton

## Location
[src/port/inet_aton.c:56-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/inet_aton.c#L56-L149)

## Overview
A portable implementation of inet_aton that converts a string representation of an IPv4 address to a binary address structure, designed to replace inet_addr with better error handling.

## Definition
```c
int inet_aton(const char *cp, struct in_addr *addr)
```

## Detailed Description
The `inet_aton` function parses ASCII string representations of IPv4 addresses and converts them to binary network byte order format. This function is part of PostgreSQL's portability layer, providing a consistent implementation across platforms that may not have native inet_aton support.

The function supports multiple IPv4 address formats:
- Standard dotted decimal notation: a.b.c.d (4 octets)
- 3-part notation: a.b.c (where c is treated as a 16-bit value)  
- 2-part notation: a.b (where b is treated as a 24-bit value)
- Single number: a (treated as a full 32-bit address)

Number parsing supports multiple bases:
- Hexadecimal numbers prefixed with 0x or 0X
- Octal numbers prefixed with 0
- Decimal numbers (default)

The function validates input strictly, rejecting malformed addresses and ensuring each component fits within its expected bit range. Unlike inet_addr(), it returns a clear success/failure indication rather than using -1 as both an error code and valid address.

## Parameters / Member Variables
- `cp`: Pointer to null-terminated string containing the ASCII representation of the IPv4 address to parse
- `addr`: Pointer to struct in_addr where the converted binary address will be stored in network byte order (can be NULL if only validation is needed)

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32
- Called from (representative examples):
  - [pq_verify_peer_name_matches_certificate_ip](../p/pq_verify_peer_name_matches_certificate_ip.md)
  - [is_ip_address](is_ip_address.md)

## Notes and Other Information
- Returns 1 on successful conversion, 0 on failure
- This is a replacement implementation for systems lacking native inet_aton()
- The function performs strict validation, rejecting addresses with trailing non-whitespace characters
- Network byte order conversion is handled via PostgreSQL's pg_hton32() function
- Used primarily in libpq for SSL certificate validation and IP address parsing
- Part of PostgreSQL's portability infrastructure (src/port/)