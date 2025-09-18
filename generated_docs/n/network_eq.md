# network_eq

## Location
src/backend/utils/adt/network.c: 814 - 822

## Overview
PostgreSQL function that implements the equality comparison operator (=) for inet and cidr data types, returning true if the two network addresses are considered equal.

## Definition
```c
Datum network_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the equality comparison operation for PostgreSQL's inet and cidr network data types. It serves as a SQL-callable wrapper around the internal network comparison logic, implementing the '=' operator for network addresses. The function extracts two inet/cidr values from the function arguments and uses the authoritative network_cmp_internal() function to determine if they are equal.

Two network addresses are considered equal when network_cmp_internal() returns 0, which occurs when all of the following components match exactly:
1. IP family (both IPv4 or both IPv6)
2. Network portions (masked bits) are identical
3. Netmask sizes are the same
4. Complete addresses (including subnet bits) are identical

For cidr types, subnet bits are always zero, so equality depends primarily on the network portion and netmask size. For inet types, the complete address including subnet bits must match. This function is typically registered in PostgreSQL's system catalogs as the implementation for the '=' operator between inet/cidr types, enabling SQL queries to perform direct equality comparisons like `inet_col1 = inet_col2`.

## Parameters / Member Variables
- Function uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- `PG_GETARG_INET_PP(0)`: First inet/cidr argument (left operand of =)
- `PG_GETARG_INET_PP(1)`: Second inet/cidr argument (right operand of =)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INET_PP`: Macro to extract inet pointer from function arguments
  - [network_cmp_internal](network_cmp_internal.md): Core network comparison function that returns ordering (-1, 0, 1)
  - `PG_RETURN_BOOL`: Macro to return boolean result from PostgreSQL function

- Called from (representative examples):
  - SQL queries using the '=' operator between inet/cidr values
  - Internal PostgreSQL operations requiring network address equality testing
  - Hash table lookups and joins involving inet/cidr columns

## Notes and Other Information
- This is a PostgreSQL function following the fmgr (function manager) calling convention
- The function is part of PostgreSQL's operator implementation infrastructure
- Returns a PostgreSQL boolean datum via PG_RETURN_BOOL macro
- The actual comparison logic is delegated to network_cmp_internal() for consistency
- Supports both inet and cidr data types transparently
- The function is typically not called directly but invoked through SQL operators
- Implements the symmetric property: if A = B, then B = A
- Used by PostgreSQL's hash-based operations when inet/cidr values serve as hash keys