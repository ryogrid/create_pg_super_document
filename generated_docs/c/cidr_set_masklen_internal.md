# cidr_set_masklen_internal

## Location
[src/backend/utils/adt/network.c:368-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L368-L404)

## Overview
Creates a copy of a network address with a specified mask length, properly handling bit masking and memory allocation for CIDR operations.

## Definition

```c
inet *
cidr_set_masklen_internal(const inet *src, int bits)
```
## Detailed Description
This function creates a new  structure by copying the source network address and setting its mask length to the specified number of bits. It performs proper bit masking to ensure that only the significant bits (according to the new mask length) are preserved in the address portion, while clearing any trailing bits. The function handles both IPv4 and IPv6 addresses and ensures proper memory allocation and variable-length header setup.

The function performs several key operations:
1. Allocates memory for a new  structure using 
2. Copies the address family from the source
3. Sets the new mask length
4. Copies only the relevant address bytes based on the mask length
5. Clears unwanted bits in partial bytes
6. Sets the proper variable-length header

## Parameters / Member Variables
- `*src`: Source  structure containing the original network address
- `bits`: New mask length in bits (must be valid for the address family)
## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (address family accessor)
  -  (mask length accessor)
  -  (maximum bits for family)
  -  (address bytes accessor)
  -  (variable-length header setup)
  -  (memory copy)
  -  (debugging assertion)

- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- The function assumes the input  parameter is valid for the address family (verified by assertion)
- Uses bit manipulation to clear unwanted bits in the last partial byte: 
- Memory is zero-initialized with , ensuring clean state for unused portions
- Essential for CIDR block operations where precise bit masking is required
- Located in 