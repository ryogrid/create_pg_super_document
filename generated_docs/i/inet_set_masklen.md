# inet_set_masklen

## Location
src/backend/utils/adt/network.c: 324 - 347

## Overview
Sets the netmask length of an INET address to a specified value, creating a new INET with the updated netmask.

## Definition
```c
Datum inet_set_masklen(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_set_masklen` function creates a new INET address with a specified netmask length while preserving the original address data. This function allows modification of the network portion specification of an IP address without changing the actual address bytes.

The function accepts an INET address and an integer specifying the desired netmask length. It validates that the netmask length is within acceptable bounds for the address family. A special value of -1 for the netmask length sets it to the maximum possible value for the address family (32 for IPv4, 128 for IPv6). The function creates a complete copy of the original INET structure and only modifies the netmask length field.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: INET pointer (`inet *`) - the source INET address
  - Argument 1: Integer (`int32`) - the desired netmask length in bits, or -1 for maximum

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP: Macro to extract INET argument from function args
  - PG_GETARG_INT32: Macro to extract int32 argument from function args
  - ip_maxbits: Function to get the maximum valid netmask length for the address family
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - memcpy: Standard library memory copy function
  - VARSIZE_ANY: Macro to get the size of a variable-length data type
  - ip_bits: Macro to access the netmask length field of an inet structure
  - ereport: PostgreSQL error reporting function
  - PG_RETURN_INET_P: Macro to return inet result
- Called from (representative examples):
  - [network_scan_last](../n/network_scan_last.md): Function for scanning network ranges

## Notes and Other Information
- This function is available as the SQL function `set_masklen(inet, int)` in PostgreSQL
- Unlike CIDR operations, this function does not mask out host bits - it only changes the netmask length field
- The special value -1 for netmask length is a convenience feature to set the maximum possible netmask
- Performs comprehensive validation with proper error reporting for invalid netmask lengths
- Creates a full copy of the input INET structure to avoid modifying the original data
- Example: `set_masklen(192.168.1.100/16, 24)` results in 192.168.1.100/24
- Located in src/backend/utils/adt/network.c:324-347