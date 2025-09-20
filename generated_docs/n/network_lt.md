# network_lt

## Location
[src/backend/utils/adt/network.c:796-804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L796-L804)

## Overview
PostgreSQL function that implements the less-than comparison operator (<) for inet and cidr data types, returning true if the first network address is less than the second.

## Definition
```c
Datum network_lt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the less-than comparison operation for PostgreSQL's inet and cidr network data types. It serves as a SQL-callable wrapper around the internal network comparison logic, implementing the '<' operator for network addresses. The function extracts two inet/cidr values from the function arguments and uses the authoritative network_cmp_internal() function to determine their ordering relationship.

The comparison follows the standard inet/cidr sorting rules:
1. IPv4 addresses are considered less than IPv6 addresses
2. Network portions (masked bits) are compared first
3. Netmask sizes are compared if network portions are equal
4. Complete addresses (including subnet bits) are compared as a final tie-breaker

This function is typically registered in PostgreSQL's system catalogs as the implementation for the '<' operator between inet/cidr types, enabling SQL queries to perform direct comparisons like `inet_col1 < inet_col2`.

## Parameters / Member Variables
- `PG_GETARG_INET_PP(0)`: First inet/cidr argument (left operand of <)
- `PG_GETARG_INET_PP(1)`: Second inet/cidr argument (right operand of <)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INET_PP`: Macro to extract inet pointer from function arguments
  - [network_cmp_internal](network_cmp_internal.md): Core network comparison function that returns ordering (-1, 0, 1)
  - `PG_RETURN_BOOL`: Macro to return boolean result from PostgreSQL function

- Called from (representative examples):
  - SQL queries using the '<' operator between inet/cidr values
  - Internal PostgreSQL operations requiring network address ordering

## Notes and Other Information
- This is a PostgreSQL function following the fmgr (function manager) calling convention
- The function is part of PostgreSQL's operator implementation infrastructure
- Returns a PostgreSQL boolean datum via PG_RETURN_BOOL macro
- The actual comparison logic is delegated to network_cmp_internal() for consistency
- Supports both inet and cidr data types transparently
- The function is typically not called directly but invoked through SQL operators