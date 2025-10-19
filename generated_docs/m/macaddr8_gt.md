# macaddr8_gt

## Location
[src/backend/utils/adt/mac8.c:374-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L374-L382)

## Overview
The macaddr8_gt function implements the greater-than comparison operator (>) for PostgreSQL's 8-byte MAC address (macaddr8) data type.

## Definition

```c
Datum
macaddr8_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares two 8-byte MAC addresses and returns true if the first MAC address is strictly greater than the second MAC address. The comparison is performed using the internal comparison function macaddr8_cmp_internal, which compares MAC addresses by first comparing the high-order bits, then the low-order bits if the high-order bits are equal. The function follows PostgreSQL's function call convention using the PG_FUNCTION_ARGS macro.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First macaddr8 value (a1) 
  - Argument 1: Second macaddr8 value (a2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro to extract macaddr8 arguments)
  - [macaddr8_cmp_internal](macaddr8_cmp_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically used as an operator function for the > operator on macaddr8 data types
- The comparison logic treats MAC addresses as 64-bit unsigned integers for ordering purposes
- Returns true (1) if a1 > a2, false (0) otherwise
- Part of PostgreSQL's MAC address data type support introduced for 8-byte MAC addresses

## Simplified Source

```c
Datum macaddr8_gt(PG_FUNCTION_ARGS) {
    macaddr8 *a1 = PG_GETARG_MACADDR8_P(0);
    macaddr8 *a2 = PG_GETARG_MACADDR8_P(1);

    // Return true if a1 > a2 (comparison result > 0)
    PG_RETURN_BOOL(macaddr8_cmp_internal(a1, a2) > 0);
}
```