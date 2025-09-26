# float4out

## Location
[src/backend/utils/adt/float.c:312-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L312-L331)

## Overview
PostgreSQL function that converts a single-precision floating-point number (float4) to its string representation using a standard output format.

## Definition

```c
Datum
float4out(PG_FUNCTION_ARGS)
```
## Detailed Description
The float4out function is a PostgreSQL built-in function that handles the conversion of float4 values to their textual representation. It uses PostgreSQL's function calling conventions (PG_FUNCTION_ARGS) and returns a Datum containing a C-string. The function supports two output modes: when extra_float_digits is greater than 0, it uses the shortest decimal representation; otherwise, it uses pg_strfromd() with a controlled number of significant digits based on FLT_DIG plus extra_float_digits.

The function allocates a 32-byte buffer for the output string, which is sufficient for representing any float4 value including special cases like infinity and NaN.

## Parameters / Member Variables
- : The float4 value extracted from the first function argument using PG_GETARG_FLOAT4(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (argument extraction)
  - [palloc](../p/palloc.md) (memory allocation)
  - [float_to_shortest_decimal_buf](float_to_shortest_decimal_buf.md) (shortest decimal conversion)
  - [pg_strfromd](../p/pg_strfromd.md) (formatted string conversion)
  - PG_RETURN_CSTRING (return value macro)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's function call mechanism)

## Notes and Other Information
- Implements PostgreSQL's standard float4 output function registered in the system catalogs
- Output format depends on the extra_float_digits setting
- Uses shortest decimal representation when extra_float_digits > 0 for more compact output
- Allocates exactly 32 bytes for the output string buffer
- Returns a palloc'd C-string that will be automatically freed by PostgreSQL's memory management