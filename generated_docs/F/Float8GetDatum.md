# Float8GetDatum

## Location
[src/backend/utils/fmgr/fmgr.c:1816-1831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1816-L1831)

## Overview
Float8GetDatum converts a double-precision floating-point value to a PostgreSQL Datum representation, handling the memory allocation required when 8-byte floats are passed by reference.

## Definition
```c
Datum Float8GetDatum(float8 X)
```

## Detailed Description
Float8GetDatum is a utility function that converts a double-precision floating-point number (float8, which is a double in C) into PostgreSQL's universal Datum type. Like Int64GetDatum, this function is only compiled when USE_FLOAT8_BYVAL is not defined, meaning that 8-byte floating-point values are passed by reference rather than by value. The function allocates memory using palloc() to store the float value and returns a pointer to this memory location wrapped as a Datum.

This function is part of PostgreSQL's type system infrastructure that provides a uniform interface for handling floating-point data types across different compilation configurations. The same control flag (USE_FLOAT8_BYVAL) governs both int8 and float8 pass-by-value behavior to maintain consistency for timestamp types that might use either representation.

## Parameters / Member Variables
- `X`: The double-precision floating-point value to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum)
- Called from (representative examples):
  - PG_RETURN_FLOAT8 (macro for returning float8 from SQL functions)
  - Statistical analysis functions
  - Geometric and mathematical functions
  - JSON path execution functions
  - Range type analysis functions
  - Time/interval arithmetic functions

## Notes and Other Information
- This function is only compiled when USE_FLOAT8_BYVAL is not defined
- When USE_FLOAT8_BYVAL is defined, Float8GetDatumFast macro is used instead for better performance
- The allocated memory is managed by PostgreSQL's memory context system
- The function works in conjunction with DatumGetFloat8() for the reverse conversion
- Shares the same compilation control as Int64GetDatum to maintain consistency for timestamp representations
- Widely used in mathematical, statistical, and geometric operations within PostgreSQL