# macaddr_out

## Location
src/backend/utils/adt/mac.c: 121 - 139

## Overview
This function converts PostgreSQL's internal macaddr data type to its string representation in a standardized colon-separated hexadecimal format.

## Definition
```c
Datum macaddr_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `macaddr_out` function is the output function for PostgreSQL's macaddr data type. It takes an internal macaddr structure and converts it to a standardized string representation. Unlike the input function which accepts multiple formats, the output function always produces MAC addresses in the canonical colon-separated lowercase hexadecimal format (xx:xx:xx:xx:xx:xx) with zero-padding for single-digit hex values.

The function allocates a fixed-size buffer of 32 characters, which is sufficient for the 17-character MAC address string plus null terminator, providing some safety margin.

## Parameters / Member Variables
- `addr`: Pointer to the input macaddr structure (retrieved via `PG_GETARG_MACADDR_P(0)`)
- `result`: Pointer to the allocated string buffer for the output

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MACADDR_P`: PostgreSQL macro to extract macaddr pointer from function arguments
  - `[palloc](../p/palloc.md)`: PostgreSQL memory allocation function
  - `snprintf`: Standard C library function for formatted string output
  - `PG_RETURN_CSTRING`: PostgreSQL macro to return C string
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL type conversion)

## Notes and Other Information
- Always outputs in lowercase hexadecimal format with colon separators
- Uses zero-padding (%02x) to ensure each octet is represented with exactly 2 digits
- Allocates 32 bytes for the result string, providing extra space beyond the required 18 bytes
- The output format is consistent regardless of the input format used when the value was created
- Memory management follows PostgreSQL conventions with palloc for allocation