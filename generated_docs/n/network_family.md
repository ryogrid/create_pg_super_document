# network_family

## Location
[src/backend/utils/adt/network.c:1266-1284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1266-L1284)

## Overview
Returns the address family version (4 for IPv4, 6 for IPv6) of a network address data type.

## Definition

```c
Datum
network_family(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL built-in function that determines the IP version of a given inet or cidr network address. It examines the address family of the input network data and returns an integer representing the IP protocol version. The function supports both IPv4 and IPv6 addresses, returning 4 for IPv4 addresses, 6 for IPv6 addresses, and 0 for any unrecognized address family.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the inet/cidr input value
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts inet argument from function arguments
  - : Determines the address family of the inet structure
  - : Network address data type
  - : Constant for IPv4 address family
  - : Constant for IPv6 address family
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- This function is exposed as a SQL function that can be called from PostgreSQL queries
- Returns 0 for any address family that is not IPv4 or IPv6, providing a safe fallback
- Part of PostgreSQL's network address and manipulation functions
- Located in src/backend/utils/adt/network.c:1266-1284

## Simplified Source

```c
Datum network_family(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);  // Extract inet/cidr value

    // Determine IP version based on address family
    switch (ip_family(ip)) {
        case PGSQL_AF_INET:
            PG_RETURN_INT32(4);  // IPv4
        case PGSQL_AF_INET6:
            PG_RETURN_INT32(6);  // IPv6
        default:
            PG_RETURN_INT32(0);  // Unknown/unsupported family
    }
}
```