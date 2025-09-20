# network_ne

## Location
[src/backend/utils/adt/network.c:841-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L841-L852)

## Overview
Implements the not-equal (!=) comparison operator for network addresses (inet/cidr types) in PostgreSQL.

## Definition

```c
Datum
network_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides the != (not equal) comparison operation for PostgreSQL's inet and cidr data types. It extracts two network address arguments from the function call context and uses the internal comparison function  to determine if the two network addresses are not equal. The function returns a boolean result indicating whether the networks are different.

The inequality determination follows the same comprehensive comparison logic used by other network comparison functions:
1. Compares the common bits of the network portion
2. Compares the length of the network mask
3. Compares the entire unmasked address

If any of these comparisons yield a non-zero result, the networks are considered not equal.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention :
- First argument (index 0):  - The left-hand side network address
- Second argument (index 1):  - The right-hand side network address
- Returns:  - Boolean result of the != comparison

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts inet arguments from function call
  -  - Internal comparison function for network addresses
  -  - Returns boolean result
  -  - Network address data type
- Called from:
  - PostgreSQL SQL engine when != or <> operator is used with inet/cidr types

## Notes and Other Information
- This function is part of PostgreSQL's operator system and is typically invoked through SQL expressions like  or 
- Returns true when networks differ in any aspect: network portion, mask length, or host portion
- The comparison logic handles both inet and cidr types uniformly
- Network family differences (IPv4 vs IPv6) will result in inequality
- Uses PostgreSQL's standard function calling convention with  and  return type
- This is the logical opposite of the network equality operator