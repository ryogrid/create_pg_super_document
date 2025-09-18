# range_sockaddr_AF_INET

## Location
src/backend/libpq/ifaddr.c: 66 - 77

## Overview
Performs IPv4-specific subnet range checking to determine if an IPv4 address falls within a specified subnet.

## Definition


## Detailed Description
This static function implements IPv4 subnet matching using bitwise operations. It performs an XOR operation between the target address and network address, then applies the netmask using a bitwise AND operation. If the result is zero, the address is within the subnet; otherwise, it's outside the subnet range. This is the standard algorithm for IPv4 subnet matching.

## Parameters / Member Variables
- : The IPv4 socket address to be checked for subnet membership
- : The IPv4 network address that defines the subnet base
- : The IPv4 netmask that defines the subnet boundaries

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - uses only standard bitwise operations)
- Called from (representative examples):
  - [pg_range_sockaddr](../p/pg_range_sockaddr.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Uses the standard IPv4 subnet matching algorithm: ((addr ^ netaddr) & netmask) == 0
- Returns 1 if the address is within the subnet, 0 otherwise
- Operates directly on the s_addr field of the sockaddr_in structure, which contains the 32-bit IPv4 address in network byte order