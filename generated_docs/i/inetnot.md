# inetnot

## Location
src/backend/utils/adt/network.c: 1857 - 1881

## Overview
Performs a bitwise NOT operation on an inet address, returning the bitwise complement of all address bits.

## Definition
```c
Datum inetnot(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the bitwise NOT operator (~) for PostgreSQL's inet data type. It takes an inet address as input and returns a new inet address where every bit in the address portion has been inverted (0 becomes 1, 1 becomes 0). The function preserves the original address family (IPv4 or IPv6), subnet mask bits, and other metadata while only inverting the actual address bytes.

The operation iterates through each byte of the IP address and applies the bitwise NOT operator (~) to invert all bits. This is primarily useful for network calculations and address manipulations.

## Parameters / Member Variables
- Input parameter (via PG_GETARG_INET_PP(0)): The inet address to perform bitwise NOT operation on

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (PostgreSQL argument retrieval macro)
  - inet (struct type for network addresses)
  - palloc0 (PostgreSQL zero-initialized memory allocation)
  - ip_addrsize (get size of IP address in bytes)
  - ip_addr (get pointer to IP address bytes)
  - ip_bits (get/set subnet mask bits)
  - ip_family (get/set address family)
  - SET_INET_VARSIZE (set variable-length type size)
  - PG_RETURN_INET_P (PostgreSQL return macro)
- Called from (representative examples):
  - No direct callers found (likely called through SQL operator interface)

## Notes and Other Information
- Implements the PostgreSQL ~ operator for inet types
- Preserves address family (IPv4/IPv6) and subnet mask information
- Only inverts the actual address bits, not the metadata
- Allocates new inet structure for the result using palloc0
- Accessible from SQL as the ~ operator (e.g., ~'192.168.1.1'::inet)
- Part of PostgreSQL's network address manipulation functions
- Useful for network address calculations and bitwise operations