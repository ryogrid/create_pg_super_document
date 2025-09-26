# ipv6eq

## Location
[src/backend/libpq/hba.c:1037-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L1037-L1051)

## Overview
Compares two IPv6 socket addresses for equality by performing a byte-by-byte comparison of their 128-bit IPv6 addresses.

## Definition

```c
static bool
ipv6eq(struct sockaddr_in6 *a, struct sockaddr_in6 *b)
```
## Detailed Description
The  function determines whether two IPv6 socket address structures represent the same IPv6 address. It performs a comprehensive byte-by-byte comparison of the entire 128-bit IPv6 address stored in the  array. 

The function iterates through all 16 bytes of the IPv6 address, comparing each byte individually. If any byte differs between the two addresses, the function immediately returns false. Only when all 16 bytes match does the function return true.

This function is used within PostgreSQL's Host-Based Authentication (HBA) system to compare client IPv6 addresses with configured address ranges or specific addresses in HBA entries, similar to its IPv4 counterpart .

## Parameters / Member Variables
- : Pointer to the first IPv6 socket address structure to compare
- : Pointer to the second IPv6 socket address structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only standard struct field access and basic control flow)
- Called from (representative examples):
  - check_hostname (in hba.c)

## Notes and Other Information
- Compares only the IPv6 address portion (sin6_addr.s6_addr), not port or other fields
- Uses byte-by-byte comparison for the full 128-bit IPv6 address (16 bytes)
- Assumes both input pointers are valid IPv6 socket address structures
- Part of the HBA hostname resolution and matching subsystem
- Early termination on first byte mismatch for efficiency
- Counterpart to the simpler  function for IPv4 addresses