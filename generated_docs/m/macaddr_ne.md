# macaddr_ne

## Location
src/backend/utils/adt/mac.c: 255 - 266

## Overview
The  function compares two MAC addresses and returns true if they are not equal.

## Definition


## Detailed Description
This function implements the not-equal comparison operator (!=) for the  data type in PostgreSQL. It extracts two MAC address arguments from the function call context and uses the internal comparison function  to determine if they are different. The function returns a boolean value indicating whether the two MAC addresses are not equal.

The comparison is performed by using , which returns 0 if the addresses are equal and non-zero if they are different. This function simply checks if the result is not equal to 0.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - First MAC address to compare
  - Second argument:  - Second MAC address to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract MAC address arguments
  - : Internal function that performs the actual comparison
  - : Macro to return a boolean result
- Called from (representative examples):
  - No direct callers found (likely used through SQL operator system)

## Notes and Other Information
- This function is part of PostgreSQL's MAC address data type implementation
- It follows the standard PostgreSQL function calling conventions using 
- The actual comparison logic is delegated to  which returns 0 for equal addresses
- Used internally by PostgreSQL's operator system to support the '!=' and '<>' operators for MAC addresses
- Located in 
- Complements the  function by providing the logical negation of equality testing