# network_le

## Location
src/backend/utils/adt/network.c: 805 - 813

## Overview
PostgreSQL function that implements the less-than-or-equal-to comparison operator (<=) for inet and cidr data types, returning true if the first network address is less than or equal to the second.

## Definition
```c
Datum network_le(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the less-than-or-equal-to comparison operation for PostgreSQL's inet and cidr network data types. It serves as a SQL-callable wrapper around the internal network comparison logic, implementing the '<=' operator for network addresses. The function extracts two inet/cidr values from the function arguments and uses the authoritative network_cmp_internal() function to determine their ordering relationship, returning true when the first operand is less than or equal to the second.

The comparison follows the standard inet/cidr sorting rules:
1. IPv4 addresses are considered less than IPv6 addresses  
2. Network portions (masked bits) are compared first
3. Netmask sizes are compared if network portions are equal
4. Complete addresses (including subnet bits) are compared as a final tie-breaker

This function returns true in two cases: when the first argument is strictly less than the second (network_cmp_internal returns < 0) or when they are equal (network_cmp_internal returns 0). This function is typically registered in PostgreSQL's system catalogs as the implementation for the '<=' operator between inet/cidr types, enabling SQL queries to perform direct comparisons like `inet_col1 <= inet_col2`.

## Parameters / Member Variables
- Function uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- `PG_GETARG_INET_PP(0)`: First inet/cidr argument (left operand of <=)
- `PG_GETARG_INET_PP(1)`: Second inet/cidr argument (right operand of <=)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INET_PP`: Macro to extract inet pointer from function arguments
  - `[network_cmp_internal](network_cmp_internal.md)`: Core network comparison function that returns ordering (-1, 0, 1)
  - `PG_RETURN_BOOL`: Macro to return boolean result from PostgreSQL function

- Called from (representative examples):
  - SQL queries using the '<=' operator between inet/cidr values
  - Internal PostgreSQL operations requiring network address ordering comparisons

## Notes and Other Information
- This is a PostgreSQL function following the fmgr (function manager) calling convention
- The function is part of PostgreSQL's operator implementation infrastructure
- Returns a PostgreSQL boolean datum via PG_RETURN_BOOL macro
- The actual comparison logic is delegated to network_cmp_internal() for consistency
- Supports both inet and cidr data types transparently
- The function is typically not called directly but invoked through SQL operators
- Implements the reflexive property: any network address is less than or equal to itself