# network_sup

## Location
src/backend/utils/adt/network.c: 933 - 947

## Overview
Implements the supernet operator (>> operator) to test whether one network is a strict supernet of another network.

## Definition
```c
Datum network_sup(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a boolean network containment test to determine if the first network argument is a strict supernet of the second network argument. For one network to be a strict supernet of another, it must have a less specific netmask (fewer bits) than the contained network, and its network address must match the contained network when compared using the supernet's netmask. This is the inverse operation of network_sub - where network_sub tests if the first argument is contained in the second, network_sup tests if the first argument contains the second. The function only compares networks of the same IP family (IPv4 or IPv6).

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access two inet/cidr arguments:
  - First argument: The potential supernet (retrieved with `PG_GETARG_INET_PP(0)`)
  - Second argument: The potential contained network (retrieved with `PG_GETARG_INET_PP(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP - retrieves inet arguments from function call
  - ip_family - gets the IP family (IPv4 or IPv6) of a network
  - ip_bits - gets the number of network bits (netmask length)
  - ip_addr - gets the network address portion
  - [bitncmp](../b/bitncmp.md) - performs bitwise comparison of network addresses
  - PG_RETURN_BOOL - returns boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely used as an operator function)

## Notes and Other Information
- Implements the >> operator for inet/cidr types
- Returns false immediately if the networks are from different IP families
- Requires the first network to have FEWER bits than the second (strict superset)
- Uses bitncmp to compare using the first network's netmask (fewer bits)
- This is a strict containment test - [equal](../e/equal.md) networks will return false
- This is the logical inverse of network_sub: A >> B is equivalent to B << A
- For example: '192.168.0.0/16' >> '192.168.1.0/24' returns true
- Used to test if a broader network contains a more specific subnet