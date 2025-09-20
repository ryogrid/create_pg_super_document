# network_broadcast

## Location
[src/backend/utils/adt/network.c:1285-1329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1285-L1329)

## Overview
Computes the broadcast address for a given network/subnet by setting all host bits to 1 within the network prefix length.

## Definition

```c
Datum
network_broadcast(PG_FUNCTION_ARGS)
```
## Detailed Description
This function calculates the broadcast address for a given inet or cidr network address. It takes a network address with a prefix length (netmask) and generates the corresponding broadcast address by setting all host bits (bits beyond the network prefix) to 1. The function works by creating a new inet structure, copying the network portion of the address, and then applying an OR operation with appropriate masks to set the host bits to 1. This is essential for network operations that need to determine the broadcast address of a subnet.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing the inet/cidr input network address

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts inet argument from function arguments
  - : Allocates zero-initialized memory for the result
  - : Gets the size of the IP address in bytes
  - : Gets the prefix length (netmask bits) of the address
  - : Gets pointer to the raw address bytes
  - : Gets/sets the address family
  - : Sets the variable size header for the inet type
  - : Returns the inet result
- Called from (representative examples):
  -  (src/backend/utils/adt/network.c:1707)

## Notes and Other Information
- The algorithm processes the address byte by byte, applying appropriate bit masks
- For each byte, it determines how many network bits remain and creates a mask accordingly
- Host bits are set to 1 using bitwise OR operations with the calculated mask
- The function preserves the original address family and prefix length in the result
- Essential for network range operations and subnet calculations
- Located in src/backend/utils/adt/network.c:1285-1329