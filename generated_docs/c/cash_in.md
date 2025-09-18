# cash_in

## Location
[src/backend/utils/adt/cash.c:173-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L173-L386)

## Overview
A PostgreSQL input function that converts a string representation to a Cash data type, supporting various currency formats and localization.

## Definition
```c
Datum cash_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function parses string representations of monetary values and converts them to PostgreSQL's internal Cash data type. It supports flexible input formats including currency symbols, thousands separators, decimal points, and various sign representations (leading/trailing minus signs, parentheses for negative values). The function uses locale-specific settings from the system's locale configuration to handle different currency formats appropriately. It performs comprehensive input validation, overflow detection, and proper rounding when necessary.

The function accumulates the absolute value in negative form to handle the full range of signed integers more safely, then applies the correct sign at the end. It supports formats like: $123.45, 123,456.78, (123.45) for negative, +123.45, -123.45, etc.

## Parameters / Member Variables
- Function takes a C-string input through PostgreSQL's function argument system (PG_GETARG_CSTRING)
- Uses error context for soft error handling when available
- Internal variables:
  - `result`: Final Cash value to return
  - `value`: Accumulated value during parsing (built in negative)
  - `dec`: Count of decimal places processed
  - `sgn`: Sign multiplier (1 for positive, -1 for negative)
  - `seen_dot`: Boolean flag tracking if decimal point encountered
  - `fpoint`: Number of fractional digits from locale
  - `dsymbol`: Decimal point character from locale
  - `ssymbol`, `psymbol`, `nsymbol`, `csymbol`: Locale-specific symbols

## Dependencies
- Functions called/Symbols referenced:
  - Cash (data type)
  - [PGLC_localeconv](../P/PGLC_localeconv.md) (locale conversion settings)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (safe multiplication with overflow detection)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (safe subtraction with overflow detection)
  - ereturn (soft error return mechanism)
  - PG_RETURN_CASH (return macro for Cash values)
  - PG_INT64_MIN (minimum 64-bit integer constant)
- Called from:
  - This appears to be a top-level input function, likely called by PostgreSQL's type system

## Notes and Other Information
- This is a PostgreSQL-style input function following the fmgr (function manager) calling convention
- Uses comprehensive locale support for international currency formats
- Implements robust overflow detection throughout the parsing process
- Handles edge cases like the most negative 64-bit integer value
- Supports both hard and soft error reporting modes via error context
- Includes debug output when compiled with CASHDEBUG flag
- Performs input validation and normalization of whitespace and currency symbols
- Uses safe arithmetic operations to prevent integer overflow vulnerabilities
- The parsing logic builds values in negative form as a safety measure against integer overflow