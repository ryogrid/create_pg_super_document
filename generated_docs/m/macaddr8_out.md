# macaddr8_out

## Location
src/backend/utils/adt/mac8.c: 234 - 253

## Overview
A PostgreSQL output function that converts the internal macaddr8 structure to its string representation in a standardized colon-separated format.

## Definition
```c
Datum macaddr8_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the output formatter for the macaddr8 PostgreSQL data type. It takes a macaddr8 structure and converts it to a human-readable string representation using a fixed format with colon separators. The output is always in 8-byte (EUI-64) format, displaying all bytes as lowercase hexadecimal pairs separated by colons (e.g., "01:23:45:67:89:ab:cd:ef").

The function uses `snprintf` with the "%02x" format specifier to ensure each byte is displayed as exactly two lowercase hexadecimal digits, padding with leading zeros when necessary. Unlike the input function which accepts various formats, the output function always uses a consistent, standardized format.

## Parameters / Member Variables
- Uses the standard PostgreSQL function calling convention via `PG_FUNCTION_ARGS`
- `addr`: Input macaddr8 structure containing the 8-byte MAC address (accessed via `PG_GETARG_MACADDR8_P(0)`)

## Dependencies
- Functions called/Symbols referenced:
  - `macaddr8`: The input data structure containing the MAC address bytes
  - `PG_GETARG_MACADDR8_P`: PostgreSQL macro for extracting macaddr8 arguments
  - `PG_RETURN_CSTRING`: PostgreSQL macro for returning C string values
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function (allocates 32 bytes for the output string)
  - `snprintf`: Standard C library function for formatted string output
- Called from:
  - PostgreSQL type system (automatically called for macaddr8-to-string conversions)

## Notes and Other Information
- Output format is always fixed as "xx:xx:xx:xx:xx:xx:xx:xx" with lowercase hexadecimal digits
- Allocates exactly 32 bytes for the output string (sufficient for 8 hex pairs + 7 colons + null terminator)
- Always outputs the full 8-byte EUI-64 format, even for addresses that were originally entered as 6-byte EUI-48
- The function is registered with PostgreSQL's type system and called automatically during output conversion
- Returns a Datum containing a null-terminated C string
- No error checking is performed as the input is assumed to be a valid macaddr8 structure