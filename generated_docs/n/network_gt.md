# network_gt

## Location
src/backend/utils/adt/network.c: 832 - 840

## Overview
Implements the greater-than (>) comparison operator for network addresses (inet/cidr types) in PostgreSQL.

## Definition


## Detailed Description
The  function provides the > comparison operation for PostgreSQL's inet and cidr data types. It extracts two network address arguments from the function call context and uses the internal comparison function  to determine if the first network address is strictly greater than the second. The function returns a boolean result indicating whether the comparison is true.

The comparison follows the same hierarchical logic as other network comparison functions:
1. First compares the common bits of the network portion
2. Then compares the length of the network mask  
3. Finally compares the entire unmasked address

This ensures consistent ordering where network addresses are primarily sorted by their network portion and secondarily by their host portion.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention :
- First argument (index 0):  - The left-hand side network address
- Second argument (index 1):  - The right-hand side network address  
- Returns:  - Boolean result of the > comparison

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts inet arguments from function call
  -  - Internal comparison function for network addresses
  -  - Returns boolean result
  -  - Network address data type
- Called from:
  - PostgreSQL SQL engine when > operator is used with inet/cidr types

## Notes and Other Information
- This function is part of PostgreSQL's operator system and is typically invoked through SQL expressions like 
- Unlike , this function requires strict inequality (returns false for equal networks)
- The comparison logic handles both inet and cidr types uniformly
- Network family differences (IPv4 vs IPv6) are handled by comparing family identifiers
- Uses PostgreSQL's standard function calling convention with  and  return type