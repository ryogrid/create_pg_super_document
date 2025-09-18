# numeric_in

## Location
src/backend/utils/adt/numeric.c: 635 - 813

## Overview
The input function for PostgreSQL's numeric data type, responsible for parsing string representations of numbers and converting them into internal Numeric format.

## Definition
```c
Datum numeric_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_in` function serves as the primary input conversion function for PostgreSQL's arbitrary precision numeric data type. It parses various string representations including:

1. **Standard decimal numbers**: Regular floating-point notation (e.g., "123.456", "-0.789")
2. **Special values**: NaN, Infinity, and -Infinity (case-insensitive)
3. **Non-decimal integers**: Hexadecimal (0x/0X), octal (0o/0O), and binary (0b/0B) prefixes
4. **Signed values**: Supports both positive (+) and negative (-) signs

The function performs comprehensive syntax validation, handles typmod constraints, and produces appropriate error messages for malformed input. It follows PostgreSQL's standard input function conventions and integrates with the soft error reporting system.

## Parameters / Member Variables
- `str`: Input string to be parsed (PG_GETARG_CSTRING(0))
- `typelem`: Type element OID (unused, PG_GETARG_OID(1))  
- `typmod`: Type modifier for precision/scale constraints (PG_GETARG_INT32(2))
- `escontext`: Error context for soft error handling (fcinfo->context)

## Dependencies
- Functions called/Symbols referenced:
  - [make_result](../m/make_result.md): Creates Numeric result from NumericVar
  - [make_result_opt_error](../m/make_result_opt_error.md): Creates Numeric result with error handling
  - [set_var_from_str](../s/set_var_from_str.md): Parses decimal string representation
  - [set_var_from_non_decimal_integer_str](../s/set_var_from_non_decimal_integer_str.md): Parses non-decimal integer strings
  - [apply_typmod](../a/apply_typmod.md): Applies type modifier constraints
  - [apply_typmod_special](../a/apply_typmod_special.md): Applies constraints to special values
  - [pg_strncasecmp](../p/pg_strncasecmp.md): Case-insensitive string comparison
  - `init_var`/`free_var`: NumericVar memory management
- Called from (representative examples):
  - [make_const](../m/make_const.md): Parser constant creation
  - [numeric_to_number](numeric_to_number.md): Formatting operations
  - [jsonb_in_scalar](../j/jsonb_in_scalar.md): JSON numeric conversion
  - `pg_lsn_*`: LSN arithmetic operations

## Notes and Other Information
- Supports both traditional decimal and modern non-decimal integer formats (hex, octal, binary)
- Implements PostgreSQL's standard approach for handling NaN and infinity values
- Integrates with the soft error reporting system for better error handling in contexts like COPY
- Performs thorough input validation including trailing whitespace checks
- The function is registered in the system catalogs and called automatically during numeric type input conversion