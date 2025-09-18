# macaddr8_ne

## Location
src/backend/utils/adt/mac8.c: 383 - 394

## Overview
The macaddr8_ne function implements the not-equal comparison operator (!=) for PostgreSQL's 8-byte MAC address (macaddr8) data type.

## Definition


## Detailed Description
This function compares two 8-byte MAC addresses and returns true if the first MAC address is not equal to the second MAC address. The comparison is performed using the internal comparison function macaddr8_cmp_internal, which compares MAC addresses by first comparing the high-order bits, then the low-order bits if the high-order bits are equal. The function follows PostgreSQL's function call convention using the PG_FUNCTION_ARGS macro.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First macaddr8 value (a1) 
  - Argument 1: Second macaddr8 value (a2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro to extract macaddr8 arguments)
  - macaddr8_cmp_internal (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically used as an operator function for the != or <> operator on macaddr8 data types
- The comparison logic treats MAC addresses as 64-bit unsigned integers for ordering purposes
- Returns true (1) if a1 != a2, false (0) otherwise
- Part of PostgreSQL's MAC address data type support introduced for 8-byte MAC addresses