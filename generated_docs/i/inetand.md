# inetand

## Location
src/backend/utils/adt/network.c: 1882 - 1913

## Overview
Performs a bitwise AND operation between two inet addresses, returning the bitwise conjunction of corresponding address bits.

## Definition
```c
Datum inetand(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the bitwise AND operator (&) for PostgreSQL's inet data type. It takes two inet addresses as input and returns a new inet address where each bit position contains the result of ANDing the corresponding bits from both input addresses. The function requires both addresses to be of the same family (both IPv4 or both IPv6) and will raise an error if they differ.

The operation iterates through each byte of the IP addresses and applies the bitwise AND operator (&). The resulting subnet mask is set to the maximum of the two input subnet masks, ensuring the result covers the more specific of the two subnets.

## Parameters / Member Variables
- Input parameter 0 (via PG_GETARG_INET_PP(0)): The first inet address for the AND operation
- Input parameter 1 (via PG_GETARG_INET_PP(1)): The second inet address for the AND operation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (PostgreSQL argument retrieval macro)
  - inet (struct type for network addresses)
  - palloc0 (PostgreSQL zero-initialized memory allocation)
  - ip_family (get address family)
  - ereport (PostgreSQL error reporting)
  - errcode/errmsg (error handling macros)
  - ip_addrsize (get size of IP address in bytes)
  - ip_addr (get pointer to IP address bytes)
  - ip_bits (get/set subnet mask bits)
  - Max (maximum value macro)
  - SET_INET_VARSIZE (set variable-length type size)
  - PG_RETURN_INET_P (PostgreSQL return macro)
- Called from (representative examples):
  - No direct callers found (likely called through SQL operator interface)

## Notes and Other Information
- Implements the PostgreSQL & operator for inet types
- Requires both operands to be of the same address family (IPv4 or IPv6)
- Raises ERRCODE_INVALID_PARAMETER_VALUE error for mismatched address families
- Result subnet mask is the maximum of the two input subnet masks
- Allocates new inet structure for the result using palloc0
- Accessible from SQL as the & operator (e.g., '192.168.1.1'::inet & '255.255.255.0'::inet)
- Part of PostgreSQL's network address manipulation functions
- Useful for network masking and address calculations