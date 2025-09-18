# macaddr_not

## Location
[src/backend/utils/adt/mac.c:287-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L287-L302)

## Overview
The  function performs a bitwise NOT operation on a MAC address, returning a new MAC address with all bits inverted.

## Definition


## Detailed Description
This function implements the bitwise NOT arithmetic operator (~) for the  data type in PostgreSQL. It takes a MAC address as input and creates a new MAC address where each byte has been bitwise inverted (all 1s become 0s and all 0s become 1s). The function allocates memory for the result and performs the bitwise NOT operation on each of the six bytes (a, b, c, d, e, f) that make up a MAC address.

This is one of PostgreSQL's arithmetic functions for MAC addresses, allowing bitwise manipulation of MAC address values. The operation is performed independently on each byte of the MAC address structure.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - The MAC address to perform bitwise NOT operation on

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract MAC address argument
  - : PostgreSQL memory allocation function
  - : Macro to return a MAC address result
  -  structure with fields: a, b, c, d, e, f (unsigned char each)
- Called from (representative examples):
  - No direct callers found (likely used through SQL operator system)

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operations for MAC addresses
- Each byte of the MAC address (a, b, c, d, e, f) is independently inverted using the bitwise NOT operator (~)
- The function allocates new memory for the result rather than modifying the input
- Used to support the ~ operator for MAC addresses in SQL expressions
- Located in 
- Part of a family of bitwise arithmetic functions including AND and OR operations for MAC addresses
- The MAC address structure consists of 6 unsigned char fields representing the standard 6-byte MAC address format