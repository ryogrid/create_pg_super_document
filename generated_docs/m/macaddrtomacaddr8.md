# macaddrtomacaddr8

## Location
src/backend/utils/adt/mac8.c: 524 - 544

## Overview
Converts a 6-byte MAC address (macaddr) to an 8-byte MAC address (macaddr8) by inserting the standard EUI-64 expansion bytes FF:FE in the middle.

## Definition
```c
Datum macaddrtomacaddr8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the conversion from a 6-byte MAC address (macaddr type) to an 8-byte MAC address (macaddr8 type) following the EUI-64 standard. The conversion works by taking the original 6-byte MAC address and inserting two standard bytes (0xFF and 0xFE) between the 3rd and 4th bytes of the original address.

The conversion follows this mapping:
- Original: aa:bb:cc:dd:ee:ff
- Result: aa:bb:cc:ff:fe:dd:ee:ff

This is a PostgreSQL SQL function that can be called from SQL queries to convert macaddr values to macaddr8 format.

## Parameters / Member Variables
- Function uses PostgreSQL's function call convention with `PG_FUNCTION_ARGS`
- Input parameter accessed via `PG_GETARG_MACADDR_P(0)`: The 6-byte MAC address to convert

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MACADDR_P`: Retrieves the input macaddr parameter
  - `palloc0`: Allocates zero-initialized memory for the result
  - `PG_RETURN_MACADDR8_P`: Returns the macaddr8 result
- Types referenced:
  - `macaddr`: Input 6-byte MAC address type
  - `macaddr8`: Output 8-byte MAC address type
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- The function implements the standard EUI-64 expansion by inserting 0xFF and 0xFE bytes at positions d and e respectively
- Memory allocation uses `palloc0` to ensure the result structure is zero-initialized
- This is the complementary function to `macaddr8tomacaddr` which performs the reverse conversion
- Located in src/backend/utils/adt/mac8.c:524-544