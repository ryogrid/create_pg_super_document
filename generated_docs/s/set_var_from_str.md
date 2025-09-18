# set_var_from_str

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 78 - 225

## Overview
A comprehensive string-to-numeric parsing function that converts textual numeric representations into PostgreSQL's internal NumericVar format, handling various formats including scientific notation and digit separators.

## Definition
```c
static bool
set_var_from_str(const char *str, const char *cp,
                 NumericVar *dest, const char **endptr,
                 Node *escontext)
```

## Detailed Description
The `set_var_from_str` function is a sophisticated parser that handles the conversion of string representations of numbers into PostgreSQL's internal NumericVar format. This function supports a wide range of numeric formats including integers, decimals, scientific notation (with 'e' or 'E'), and numbers with underscore digit separators for improved readability. The parsing process involves two main phases: first extracting decimal digits and determining the decimal weight, then converting to PostgreSQL's NBASE representation. The function provides comprehensive error handling and can work with PostgreSQL's soft error reporting system through the escontext parameter.

## Parameters / Member Variables
- `str`: The original string for error reporting purposes
- `cp`: The actual parsing start position (typically after skipping leading spaces)
- `dest`: Pointer to the NumericVar structure that will receive the parsed numeric value
- `endptr`: Returns the position after the last parsed character
- `escontext`: Error context for soft error reporting (can be NULL for traditional error throwing)

## Dependencies
- Functions called/Symbols referenced:
  - [alloc_var](../a/alloc_var.md) (allocates digit buffer for the result)
  - [strip_var](strip_var.md) (normalizes the result by removing leading/trailing zeros)
  - [palloc](../p/palloc.md) (allocates temporary buffer for decimal digits)
  - [pfree](../p/pfree.md) (frees temporary buffer)
  - ereturn (soft error reporting)
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT
  - [numeric_in](../n/numeric_in.md) (main numeric input function)
  - [float8_numeric](../f/float8_numeric.md) (float to numeric conversion)
  - [float4_numeric](../f/float4_numeric.md) (float to numeric conversion)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md) (ECPG library)

## Notes and Other Information
- Supports scientific notation with both positive and negative exponents
- Handles underscore separators within numbers (but not after decimal points or before digits)
- Uses a two-phase parsing approach: decimal extraction followed by NBASE conversion
- Supports both traditional error throwing and soft error reporting via escontext
- The function does not handle leading or trailing whitespace - this must be done by the caller
- Returns both success/failure status and the end position for further parsing by callers
- Implements strict validation to prevent various forms of malformed input
- The temporary decimal digit buffer includes padding for proper alignment during NBASE conversion