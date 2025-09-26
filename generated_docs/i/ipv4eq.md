# ipv4eq

## Location
[src/backend/libpq/hba.c:1031-1036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L1031-L1036)

## Overview
Compares two IPv4 socket addresses for equality by checking if their IP addresses are identical.

## Definition

```c
static bool
ipv4eq(struct sockaddr_in *a, struct sockaddr_in *b)
```
## Detailed Description
The  function is a simple utility function that determines whether two IPv4 socket address structures represent the same IP address. It performs a direct comparison of the  field, which contains the 32-bit IPv4 address in network byte order.

This function is used within PostgreSQL's Host-Based Authentication (HBA) system to compare client IP addresses with configured address ranges or specific addresses in HBA entries. The function only compares the IP address portion and ignores other socket address fields like port numbers.

## Parameters / Member Variables
- : Pointer to the first IPv4 socket address structure to compare
- : Pointer to the second IPv4 socket address structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only standard struct field access)
- Called from (representative examples):
  - [check_hostname](../c/check_hostname.md) (in hba.c)

## Notes and Other Information
- Only compares the IP address portion (sin_addr.s_addr), not port or other fields
- Assumes both input pointers are valid IPv4 socket address structures
- Uses direct integer comparison since sin_addr.s_addr is a 32-bit value
- Part of the HBA hostname resolution and matching subsystem
- Simple helper function with no error checking or validation