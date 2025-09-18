# inet_to_cidr

## Location
src/backend/utils/adt/network.c: 309 - 323

## Overview
Converts an INET data type to a CIDR data type, creating a network specification from a host address.

## Definition
```c
Datum inet_to_cidr(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_to_cidr` function converts an INET address (which represents a specific host) into a CIDR block representation. This conversion preserves the network portion of the address according to its netmask, effectively creating a network specification rather than a host address.

The function extracts the netmask length from the source INET address and uses it to create a proper CIDR representation. It performs validation to ensure the netmask length is within valid bounds for the address family (IPv4 or IPv6). The actual conversion work is delegated to `cidr_set_masklen_internal`, which creates a new INET structure with the network portion properly masked.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: INET pointer (`inet *`) - the source INET address to convert to CIDR format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP: Macro to extract INET argument from function args
  - ip_bits: Function to get the netmask length from an inet address
  - ip_maxbits: Function to get the maximum valid netmask length for the address family
  - [cidr_set_masklen_internal](../c/cidr_set_masklen_internal.md): Internal function that creates a new CIDR with specified mask length
  - PG_RETURN_INET_P: Macro to return inet result
  - elog: Error logging function for validation failures
- Called from (representative examples):
  - No direct callers found in codebase (likely called through PostgreSQL SQL function interface)

## Notes and Other Information
- This function is available as the SQL function `inet_to_cidr()` in PostgreSQL
- The conversion creates a network specification by zeroing out host bits beyond the netmask
- Performs safety validation to ensure the netmask length is between 0 and the maximum for the address family (32 for IPv4, 128 for IPv6)
- The result is semantically a CIDR block but uses the same internal `inet` structure as INET types
- Example: converting 192.168.1.100/24 would result in 192.168.1.0/24
- Located in src/backend/utils/adt/network.c:309-323