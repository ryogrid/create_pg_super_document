# range_sockaddr_AF_INET6

## Location
[src/backend/libpq/ifaddr.c:78-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/ifaddr.c#L78-L104)

## Overview
Performs IPv6-specific subnet range checking to determine if an IPv6 address falls within a specified subnet.

## Definition


## Detailed Description
This static function implements IPv6 subnet matching by iterating through all 16 bytes of the IPv6 address. For each byte, it performs the same bitwise operation as the IPv4 version: XOR the target address byte with the network address byte, then apply the corresponding netmask byte using bitwise AND. If any byte comparison yields a non-zero result, the address is outside the subnet. Only if all 16 bytes pass the test is the address considered within the subnet range.

## Parameters / Member Variables
- : The IPv6 socket address to be checked for subnet membership
- : The IPv6 network address that defines the subnet base
- : The IPv6 netmask that defines the subnet boundaries

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - uses only standard bitwise operations and loop control)
- Called from (representative examples):
  - [pg_range_sockaddr](../p/pg_range_sockaddr.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Uses the IPv6 subnet matching algorithm by applying the IPv4 algorithm to each of the 16 address bytes
- Returns 1 if the address is within the subnet, 0 otherwise
- The loop iterates through all 16 bytes of the IPv6 address (s6_addr array)
- Uses early exit optimization: returns 0 immediately when any byte fails the subnet test