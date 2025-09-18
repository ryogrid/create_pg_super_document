# addressOK

## Location
[src/backend/utils/adt/network.c:1641-1689](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1641-L1689)

## Overview
Verifies that a CIDR address is valid by ensuring it doesn't have bits set beyond the specified mask length.

## Definition


## Detailed Description
This function validates CIDR (Classless Inter-Domain Routing) addresses by checking that no host bits are set beyond the network mask length. For a valid CIDR address, all bits after the mask length should be zero. The function supports both IPv4 and IPv6 address families and performs bitwise validation to ensure the address conforms to CIDR standards.

The validation process involves:
1. Determining the maximum bits and bytes based on address family (32 bits/4 bytes for IPv4, 128 bits/16 bytes for IPv6)
2. Calculating which byte contains the last significant bit of the mask
3. Creating appropriate bit masks to check for extraneous bits
4. Iterating through bytes beyond the mask length to ensure they are zero

## Parameters / Member Variables
- : Pointer to the byte array containing the network address
- : The number of significant bits in the network mask (mask length)
- : Address family identifier (PGSQL_AF_INET for IPv4, or IPv6)

## Dependencies
- Functions called/Symbols referenced:
  - PGSQL_AF_INET (constant for IPv4 address family)
  - Assert (assertion macro)

- Called from (representative examples):
  - [network_in](../n/network_in.md) (network address input function)
  - [network_recv](../n/network_recv.md) (network address receive function)

## Notes and Other Information
- This is a static function, only accessible within the network.c source file
- The function assumes the bits parameter is valid (≤ maxbits for the family)
- Returns true if the address is valid CIDR format, false if host bits are set beyond the mask
- Critical for ensuring network address data integrity in PostgreSQL's network data types