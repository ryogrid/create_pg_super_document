# euc_jis_2004_to_shift_jis_2004

## Location
src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c: 40 - 55

## Overview
PostgreSQL function that converts character encoding from EUC-JIS-2004 to Shift-JIS-2004 encoding.

## Definition
```c
Datum euc_jis_2004_to_shift_jis_2004(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion function wrapper that converts text from EUC-JIS-2004 encoding to Shift-JIS-2004 encoding. It follows the standard PostgreSQL conversion function interface, accepting source and destination buffers along with conversion parameters. The function validates the encoding conversion arguments and delegates the actual conversion work to the `euc_jis_20042shift_jis_2004` helper function.

## Parameters / Member Variables
- `PG_GETARG_CSTRING(2)`: Source string in EUC-JIS-2004 encoding (null-terminated C string)
- `PG_GETARG_CSTRING(3)`: Destination buffer for converted Shift-JIS-2004 string
- `PG_GETARG_INT32(4)`: Length of the source string in bytes
- `PG_GETARG_BOOL(5)`: Error handling flag - if true, don't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [euc_jis_20042shift_jis_2004](euc_jis_20042shift_jis_2004.md)
  - PG_RETURN_INT32
- Constants used:
  - PG_EUC_JIS_2004
  - PG_SHIFT_JIS_2004
- Called from: This function is not directly referenced in the codebase but is likely called through PostgreSQL's conversion function registry

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:40-55
- Returns the number of bytes successfully converted as an INTEGER
- Uses PostgreSQL's standard function argument macros for parameter extraction
- Performs encoding validation before attempting conversion
- Part of PostgreSQL's multibyte character encoding conversion system