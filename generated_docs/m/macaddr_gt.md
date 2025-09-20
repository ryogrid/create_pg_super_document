# macaddr_gt

## Location
[src/backend/utils/adt/mac.c:246-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L246-L254)

## Overview
The  function compares two MAC addresses and returns true if the first address is greater than the second address in lexicographical order.

## Definition

```c
Datum
macaddr_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the greater-than comparison operator (>) for the  data type in PostgreSQL. It extracts two MAC address arguments from the function call context and uses the internal comparison function  to determine their relative order. The function returns a boolean value indicating whether the first MAC address is lexicographically greater than the second.

The comparison is performed by comparing the high-order bits first, then the low-order bits if the high-order bits are equal. This provides a consistent total ordering for MAC addresses.

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
- The actual comparison logic is delegated to  which compares high and low order bits sequentially
- Used internally by PostgreSQL's operator system to support the '>' operator for MAC addresses
- Located in 