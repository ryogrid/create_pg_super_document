# macaddr_le

## Location
src/backend/utils/adt/mac.c: 219 - 227

## Overview
PostgreSQL function that determines if the first MAC address is less than or equal to the second MAC address, returning a boolean result.

## Definition


## Detailed Description
The macaddr_le function implements the less-than-or-equal-to comparison operator ('<=') for MAC address data types in PostgreSQL. It takes two MAC address arguments and returns true if the first MAC address is lexicographically smaller than or equal to the second, false otherwise. The function uses macaddr_cmp_internal to perform the comparison and checks if the result is less than or equal to zero (<= 0).

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing two macaddr pointers
  - Argument 0: First MAC address (macaddr*)
  - Argument 1: Second MAC address to compare against (macaddr*)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR_P (extracts macaddr argument from function args)
  - macaddr_cmp_internal (performs the actual comparison logic)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of the boolean comparison operators for MAC addresses
- Uses the same hierarchical comparison logic as macaddr_cmp (high bits first, then low bits)
- Supports SQL operations like 'WHERE mac_column <= 'aa:bb:cc:dd:ee:ff''
- Returns true when addresses are equal (unlike macaddr_lt)
- Located in src/backend/utils/adt/mac.c:219-227