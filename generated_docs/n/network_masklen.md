# network_masklen

## Location
[src/backend/utils/adt/network.c:1258-1265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1258-L1265)

## Overview
Extracts and returns the netmask length (number of network bits) from an inet or cidr value as an integer.

## Definition

```c
Datum
network_masklen(PG_FUNCTION_ARGS)
```
## Detailed Description
The network_masklen function is a simple utility that extracts the netmask length from an inet or cidr value and returns it as a 32-bit integer. The netmask length represents the number of significant network bits in the address (e.g., 24 for a /24 network, 16 for a /16 network). This function provides direct access to the netmask portion of network data types, which is useful for network calculations, filtering, and analysis operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: inet/cidr value (accessed via PG_GETARG_INET_PP(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (to extract inet/cidr argument)
  - ip_bits (to get the netmask length from the inet structure)
  - PG_RETURN_INT32 (to return the integer result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/network.c:1258-1265
- Very simple function with minimal overhead - just extracts and returns the netmask length
- Returns the value as a 32-bit signed integer
- Works with both inet and cidr data types since they share the same internal structure
- Useful for network operations that need to know the subnet size or perform CIDR calculations
- The returned value represents the number of '1' bits in the netmask (e.g., 255.255.255.0 = /24)

## Simplified Source

```c
Datum network_masklen(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);  // Extract inet/cidr value

    // Return the netmask length (number of network bits)
    PG_RETURN_INT32(ip_bits(ip));
}
```