# pg_lsn_pli

## Location
src/backend/utils/adt/pg_lsn.c: 251 - 284

## Overview
The pg_lsn_pli function adds a numeric byte offset to a PostgreSQL Log Sequence Number (LSN), returning a new LSN value, and handles both positive and negative byte offsets.

## Definition
```c
Datum pg_lsn_pli(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements addition of a numeric value to an LSN, effectively allowing LSN arithmetic with arbitrary precision numeric values. It serves as the implementation for the LSN + numeric operator in PostgreSQL SQL.

The function converts the input LSN to a numeric value, performs the addition using PostgreSQL's numeric arithmetic (which supports arbitrary precision), and then converts the result back to an LSN. This approach ensures that large numeric values can be added to LSNs without overflow issues.

The function includes error handling for NaN (Not a Number) inputs, as adding NaN to an LSN would produce an invalid result. The implementation supports both positive offsets (advancing the LSN forward) and negative offsets (moving the LSN backward), making it useful for LSN calculations in replication, backup, and WAL management scenarios.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument mechanism:
- Argument 0: Base LSN value - retrieved using PG_GETARG_LSN(0)
- Argument 1: Numeric byte offset to add - retrieved using PG_GETARG_NUMERIC(1)
- Internal variables:
  - `num`: Datum holding the LSN converted to numeric format
  - `res`: Datum holding the result of numeric addition
  - `buf[32]`: Buffer for string formatting the LSN value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN argument)
  - PG_GETARG_NUMERIC (macro for extracting numeric argument)
  - numeric_is_nan (checks if numeric value is NaN)
  - UINT64_FORMAT (format string for 64-bit unsigned integers)
  - DirectFunctionCall3/2/1 (PostgreSQL function call mechanisms)
  - numeric_in (converts string to numeric type)
  - numeric_add (adds two numeric values)
  - numeric_pg_lsn (converts numeric back to LSN type)
  - CStringGetDatum/NumericGetDatum (type conversion functions)
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL's operator mechanism)

## Notes and Other Information
- Implements the LSN + numeric operator in PostgreSQL
- Supports both positive and negative numeric offsets
- Uses arbitrary precision arithmetic to avoid overflow issues
- Includes explicit NaN handling with appropriate error reporting
- Critical for LSN calculations in replication lag monitoring and WAL position arithmetic
- The conversion through numeric type ensures precision is maintained for large values
- Located in src/backend/utils/adt/pg_lsn.c:251-284