# network_subeq

## Location
src/backend/utils/adt/network.c: 918 - 932

## Overview
Implements the subnet containment or equality operator (<<= operator) to test whether one network is a subnet of or equal to another network.

## Definition
```c
Datum network_subeq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a boolean network inclusion test to determine if the first network argument is a subnet of or equal to the second network argument. Unlike the strict subnet test (network_sub), this function returns true for both strict subnets and equal networks. For one network to be contained within another, it must have an equal or more specific netmask (same or more bits) than the containing network, and its network address must match the containing network when compared using the containing network's netmask. The function only compares networks of the same IP family (IPv4 or IPv6).

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access two inet/cidr arguments:
  - First argument: The potential subnet or equal network (retrieved with `PG_GETARG_INET_PP(0)`)
  - Second argument: The potential containing network (retrieved with `PG_GETARG_INET_PP(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP - retrieves inet arguments from function call
  - ip_family - gets the IP family (IPv4 or IPv6) of a network
  - ip_bits - gets the number of network bits (netmask length)
  - ip_addr - gets the network address portion
  - bitncmp - performs bitwise comparison of network addresses
  - PG_RETURN_BOOL - returns boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely used as an operator function)

## Notes and Other Information
- Implements the <<= operator for inet/cidr types
- Returns false immediately if the networks are from different IP families
- Requires the first network to have greater than or equal bits compared to the second (>= comparison)
- Uses bitncmp to compare only the relevant bits according to the containing network's netmask
- This is a non-strict containment test - equal networks will return true
- The key difference from network_sub is the use of >= instead of > for bit comparison
- Allows for both proper subnet relationships and network equality