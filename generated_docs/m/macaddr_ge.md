# macaddr_ge

## Location
[src/backend/utils/adt/mac.c:237-245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L237-L245)

## Overview
PostgreSQL function that determines if the first MAC address is greater than or equal to the second MAC address, returning a boolean result.

## Definition

```c
Datum
macaddr_ge(PG_FUNCTION_ARGS)
```
## Detailed Description
The macaddr_ge function implements the greater-than-or-equal-to comparison operator ('>=') for MAC address data types in PostgreSQL. It takes two MAC address arguments and returns true if the first MAC address is lexicographically greater than or equal to the second, false otherwise. The function uses macaddr_cmp_internal to perform the comparison and checks if the result is greater than or equal to zero (>= 0).

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing two macaddr pointers
  - Argument 0: First MAC address (macaddr*)
  - Argument 1: Second MAC address to compare against (macaddr*)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR_P (extracts macaddr argument from function args)
  - [macaddr_cmp_internal](macaddr_cmp_internal.md) (performs the actual comparison logic)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of the boolean comparison operators for MAC addresses
- Uses the same hierarchical comparison logic as macaddr_cmp (high bits first, then low bits)
- Supports SQL operations like 'WHERE mac_column >= 'aa:bb:cc:dd:ee:ff''
- Returns true when addresses are equal (unlike macaddr_gt)
- Complements macaddr_le to provide complete ordering operations for MAC addresses
- Located in src/backend/utils/adt/mac.c:237-245

## Simplified Source

```c
Datum macaddr_ge(PG_FUNCTION_ARGS) {
    // Extract MAC address arguments
    macaddr *a1 = PG_GETARG_MACADDR_P(0);
    macaddr *a2 = PG_GETARG_MACADDR_P(1);

    // Compare MAC addresses and return true if first >= second
    PG_RETURN_BOOL(macaddr_cmp_internal(a1, a2) >= 0);
}
```