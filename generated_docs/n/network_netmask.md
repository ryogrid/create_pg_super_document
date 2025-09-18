# network_netmask

## Location
src/backend/utils/adt/network.c: 1374 - 1415

## Overview
Generates the netmask (subnet mask) for a given network address, returning an address where network bits are set to 1 and host bits are set to 0.

## Definition


## Detailed Description
This function creates the netmask (subnet mask) corresponding to a given inet or cidr network address. It takes an IP address with a prefix length and generates the appropriate netmask by setting the first N bits (where N is the prefix length) to 1 and the remaining bits to 0. The resulting address represents the subnet mask that can be used for network calculations, routing, and determining which portion of an IP address represents the network versus the host. The function sets the bits field to the maximum possible for the address family (32 for IPv4, 128 for IPv6).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing the inet/cidr input network address

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts inet argument from function arguments
  - : Allocates zero-initialized memory for the result
  - : Gets the prefix length (netmask bits) of the address
  - : Gets pointer to the raw address bytes
  - : Gets/sets the address family
  - : Gets the maximum possible bits for the address family
  - : Sets the variable size header for the inet type
  - : Returns the inet result
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- Creates a traditional subnet mask representation where network bits are 1 and host bits are 0
- The algorithm processes bytes sequentially, setting appropriate bit patterns
- For partial bytes (when prefix length is not a multiple of 8), uses left-shift operations to create the correct mask
- Sets the bits field to maximum (32 for IPv4, 128 for IPv6) rather than preserving the original prefix length
- Essential for network administration and subnet calculations
- Part of PostgreSQL's comprehensive network address manipulation functions
- Located in src/backend/utils/adt/network.c:1374-1415