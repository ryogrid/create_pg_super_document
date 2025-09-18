# pg_range_sockaddr

## Location
src/backend/libpq/ifaddr.c: 49 - 65

## Overview
Determines if a given address falls within the subnet specified by a network address and netmask.

## Definition


## Detailed Description
This function performs subnet matching by checking if a given socket address lies within the range defined by a network address and netmask. It acts as a dispatcher function that handles both IPv4 and IPv6 address families by delegating to family-specific range checking functions. The function assumes that all three addresses belong to the same address family and that AF_UNIX addresses are not supported.

## Parameters / Member Variables
- : The socket address to be checked for inclusion in the subnet
- : The network address defining the base of the subnet  
- : The netmask that defines the subnet range

## Dependencies
- Functions called/Symbols referenced:
  - range_sockaddr_AF_INET
  - range_sockaddr_AF_INET6
- Called from (representative examples):
  - check_ip
  - IFADDR_H

## Notes and Other Information
- The caller must verify that all three addresses are in the same address family before calling this function
- AF_UNIX addresses are explicitly not supported
- Returns 0 for unsupported address families
- Returns non-zero if the address is within the specified subnet range