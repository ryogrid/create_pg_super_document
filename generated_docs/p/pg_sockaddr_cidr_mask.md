# pg_sockaddr_cidr_mask

## Location
[src/backend/libpq/ifaddr.c:105-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/ifaddr.c#L105-L180)

## Overview
Creates a network mask of the appropriate address family with a specified number of significant bits for CIDR subnet calculations.

## Definition


## Detailed Description
This function generates network masks for IPv4 and IPv6 addresses based on CIDR notation. It takes a string representation of the number of network bits and converts it into a properly formatted network mask for the specified address family. For IPv4, it creates a 32-bit mask, and for IPv6, it creates a 128-bit mask by setting the appropriate number of leading bits. If numbits is NULL, it creates a full mask (32 bits for IPv4, 128 bits for IPv6). The function handles bit manipulation carefully to avoid non-portable operations like shifting by 32 bits.

## Parameters / Member Variables
- : Output parameter where the generated network mask will be stored
- : String representation of the number of network bits (can be NULL for full mask)
- : Address family (AF_INET for IPv4, AF_INET6 for IPv6)

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32
- Called from (representative examples):
  - [check_network_callback](../c/check_network_callback.md)
  - [parse_hba_line](parse_hba_line.md)
  - [run_ifaddr_callback](../r/run_ifaddr_callback.md)
  - IFADDR_H

## Notes and Other Information
- Returns 0 on success, -1 on error (invalid bit count or unsupported address family)
- For IPv4: accepts 0-32 bits, creates a 32-bit mask using bit shifting
- For IPv6: accepts 0-128 bits, creates a 128-bit mask by setting bytes iteratively
- Handles edge cases carefully, such as avoiding "x << 32" which is not portable
- Uses pg_hton32() to convert the IPv4 mask to network byte order
- Sets the ss_family field in the output mask structure
- Input validation includes checking that numbits contains only numeric characters