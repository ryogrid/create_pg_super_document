# internal_inetpl

## Location
src/backend/utils/adt/network.c: 1946 - 1997

## Overview
A static helper function that performs addition of a signed 64-bit integer value to an inet address, handling IP address arithmetic with proper overflow detection and carry propagation.

## Definition
static inet *internal_inetpl(inet *ip, int64 addend)

## Detailed Description
The `internal_inetpl` function implements IP address arithmetic by adding a 64-bit signed integer to an inet address structure. It performs byte-by-byte addition starting from the least significant byte, propagating carries through the address bytes. The function carefully handles both positive and negative addends, ensuring proper arithmetic overflow detection.

The implementation includes sophisticated overflow checking - after processing all bytes, it verifies that the final state has either zero addend and carry (for positive original addend) or -1 addend and carry 1 (for negative original addend). Any other combination indicates arithmetic overflow and triggers an error.

The function preserves the original address family and netmask bits while creating a new inet structure with the computed result.

## Parameters / Member Variables
- `ip`: Input inet address structure to which the addend will be added
- `addend`: Signed 64-bit integer value to add to the IP address

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - ip_addrsize
  - ip_addr
  - ip_bits
  - ip_family
  - SET_INET_VARSIZE
  - ereport
- Called from (representative examples):
  - [inetpl](inetpl.md)
  - [inetmi_int8](inetmi_int8.md)

## Notes and Other Information
- Uses careful bit manipulation to avoid platform-specific right-shift behavior on negative numbers
- Implements robust overflow detection for both positive and negative arithmetic
- Preserves the original inet structure's family and netmask while computing new address
- Memory allocation uses palloc0 to ensure proper PostgreSQL memory management
- Part of PostgreSQL's network data type arithmetic operations infrastructure