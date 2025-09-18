# macaddr_eq

## Location
[src/backend/utils/adt/mac.c:228-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L228-L236)

## Overview
PostgreSQL function that determines if two MAC addresses are equal, returning a boolean result.

## Definition


## Detailed Description
The macaddr_eq function implements the equality comparison operator ('=') for MAC address data types in PostgreSQL. It takes two MAC address arguments and returns true if they are identical, false otherwise. The function uses macaddr_cmp_internal to perform the comparison and checks if the result equals zero (== 0), indicating that both MAC addresses have the same high-order and low-order bits.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing two macaddr pointers
  - Argument 0: First MAC address (macaddr*)
  - Argument 1: Second MAC address to compare for equality (macaddr*)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR_P (extracts macaddr argument from function args)
  - [macaddr_cmp_internal](macaddr_cmp_internal.md) (performs the actual comparison logic)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is the equality operator for MAC addresses in SQL queries
- Uses the same hierarchical comparison logic as other macaddr comparison functions
- Supports SQL operations like 'WHERE mac_column = 'aa:bb:cc:dd:ee:ff'' and JOIN conditions
- Essential for hash-based operations and unique constraints on MAC address columns
- Located in src/backend/utils/adt/mac.c:228-236