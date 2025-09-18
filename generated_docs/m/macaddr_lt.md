# macaddr_lt

## Location
src/backend/utils/adt/mac.c: 210 - 218

## Overview
PostgreSQL function that determines if the first MAC address is less than the second MAC address, returning a boolean result.

## Definition


## Detailed Description
The macaddr_lt function implements the less-than comparison operator ('<') for MAC address data types in PostgreSQL. It takes two MAC address arguments and returns true if the first MAC address is lexicographically smaller than the second, false otherwise. The function uses the same comparison logic as macaddr_cmp by calling macaddr_cmp_internal and checking if the result is negative (< 0).

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
- Supports SQL operations like 'WHERE mac_column < 'aa:bb:cc:dd:ee:ff''
- Located in src/backend/utils/adt/mac.c:210-218