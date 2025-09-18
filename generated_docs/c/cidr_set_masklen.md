# cidr_set_masklen

## Location
src/backend/utils/adt/network.c: 348 - 367

## Overview
Sets the netmask length of a CIDR address to a specified value, creating a new CIDR with proper network masking applied.

## Definition
```c
Datum cidr_set_masklen(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cidr_set_masklen` function creates a new CIDR address with a specified netmask length, properly masking the network address to conform to CIDR specifications. Unlike `inet_set_masklen`, this function not only changes the netmask length but also ensures that host bits beyond the netmask are zeroed out, creating a proper network specification.

The function accepts a CIDR address and an integer specifying the desired netmask length. It validates that the netmask length is within acceptable bounds for the address family. A special value of -1 for the netmask length sets it to the maximum possible value for the address family (32 for IPv4, 128 for IPv6). The actual work is delegated to `cidr_set_masklen_internal`, which creates a properly masked network address.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: INET pointer (`inet *`) - the source CIDR address (internally represented as inet structure)
  - Argument 1: Integer (`int32`) - the desired netmask length in bits, or -1 for maximum

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP: Macro to extract INET argument from function args
  - PG_GETARG_INT32: Macro to extract int32 argument from function args
  - ip_maxbits: Function to get the maximum valid netmask length for the address family
  - cidr_set_masklen_internal: Internal function that creates a properly masked CIDR with specified mask length
  - ereport: PostgreSQL error reporting function
  - PG_RETURN_INET_P: Macro to return inet result
- Called from (representative examples):
  - No direct callers found in codebase (likely called through PostgreSQL SQL function interface)

## Notes and Other Information
- This function is available as the SQL function `set_masklen(cidr, int)` in PostgreSQL
- Unlike `inet_set_masklen`, this function masks out host bits beyond the specified netmask length
- The special value -1 for netmask length is a convenience feature to set the maximum possible netmask
- Performs comprehensive validation with proper error reporting for invalid netmask lengths
- Creates a new CIDR structure with proper network masking applied via `cidr_set_masklen_internal`
- Example: `set_masklen(192.168.1.100/16, 24)` results in 192.168.1.0/24 (note the host bits are zeroed)
- This function ensures CIDR semantic correctness by maintaining the network-only representation
- Located in src/backend/utils/adt/network.c:348-367