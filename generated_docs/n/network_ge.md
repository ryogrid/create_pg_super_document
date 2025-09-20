# network_ge

## Location
[src/backend/utils/adt/network.c:823-831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L823-L831)

## Overview
Implements the greater-than-or-equal-to (>=) comparison operator for network addresses (inet/cidr types) in PostgreSQL.

## Definition

```c
Datum
network_ge(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides the >= comparison operation for PostgreSQL's inet and cidr data types. It extracts two network address arguments from the function call context and uses the internal comparison function  to determine if the first network address is greater than or equal to the second. The function returns a boolean result indicating whether the comparison is true.

Network comparison in PostgreSQL follows a specific hierarchy:
1. First compares the common bits of the network portion
2. Then compares the length of the network mask
3. Finally compares the entire unmasked address

This ensures that network addresses are sorted with the network portion as the primary key and the host portion as the secondary key.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention :
- First argument (index 0):  - The left-hand side network address
- Second argument (index 1):  - The right-hand side network address
- Returns:  - Boolean result of the >= comparison

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts inet arguments from function call
  -  - Internal comparison function for network addresses
  -  - Returns boolean result
  -  - Network address data type
- Called from:
  - PostgreSQL SQL engine when >= operator is used with inet/cidr types

## Notes and Other Information
- This function is part of PostgreSQL's operator system and is typically not called directly but invoked through SQL expressions like 
- The comparison logic handles both inet and cidr types uniformly
- Network family (IPv4 vs IPv6) is considered in comparisons, with different families compared by their family identifiers
- The function uses PostgreSQL's standard function calling convention with  and  return type