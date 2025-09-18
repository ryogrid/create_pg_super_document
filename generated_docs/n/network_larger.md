# network_larger

## Location
[src/backend/utils/adt/network.c:865-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L865-L879)

## Overview
A support function for MIN/MAX operations that returns the larger of two network addresses (inet/cidr types) in PostgreSQL.

## Definition


## Detailed Description
The  function is a utility function designed to support MIN/MAX aggregate operations on network address types. It compares two network addresses using the internal comparison function and returns whichever network address is considered larger according to PostgreSQL's network address ordering rules.

The function uses  to perform the comparison, following the same hierarchical comparison strategy as other network functions:
1. First compares the common bits of the network portion
2. Then compares the length of the network mask  
3. Finally compares the entire unmasked address

If the first argument is larger (comparison result > 0), it returns the first argument; otherwise, it returns the second argument. This ensures that the function always returns the lexicographically larger network address.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention :
- First argument (index 0):  - The first network address to compare
- Second argument (index 1):  - The second network address to compare
- Returns:  - The larger of the two network addresses as an inet value

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts inet arguments from function call
  -  - Internal comparison function for network addresses
  -  - Returns inet value as result
  -  - Network address data type
- Called from:
  - PostgreSQL aggregate system for MAX operations on inet/cidr columns
  - Internal PostgreSQL functions that need to determine the larger of two networks

## Notes and Other Information
- This function is specifically designed as a support function for MIN/MAX aggregates, not for direct SQL operator use
- Unlike comparison operators that return boolean values, this function returns the actual network address
- The comparison logic is consistent with other network comparison functions in PostgreSQL
- Handles both inet and cidr types uniformly
- Network family (IPv4 vs IPv6) is considered in the comparison logic  
- Complementary function to  for aggregate operations
- Part of PostgreSQL's internal infrastructure for aggregate functions on network types