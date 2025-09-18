# network_sub

## Location
[src/backend/utils/adt/network.c:903-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L903-L917)

## Overview
Implements the subnet containment operator (<< operator) to test whether one network is a strict subnet of another.

## Definition
```c
Datum network_sub(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a boolean network inclusion test to determine if the first network argument is a strict subnet of the second network argument. For one network to be a strict subnet of another, it must have a more specific netmask (more bits) than the containing network, and its network address must match the containing network when compared using the containing network's netmask. The function only compares networks of the same IP family (IPv4 or IPv6).

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access two inet/cidr arguments:
  - First argument: The potential subnet (retrieved with `PG_GETARG_INET_PP(0)`)
  - Second argument: The potential containing network (retrieved with `PG_GETARG_INET_PP(1)`)

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
- Implements the << operator for inet/cidr types
- Returns false immediately if the networks are from different IP families
- Requires the first network to have MORE bits than the second (strict subset)
- Uses bitncmp to compare only the relevant bits according to the containing network's netmask
- This is a strict containment test - [equal](../e/equal.md) networks will return false
- For non-strict containment (<=), use network_subeq instead