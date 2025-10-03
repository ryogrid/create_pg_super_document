# float8in_internal

## Location
[src/backend/utils/adt/float.c:388-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L388-L514)

## Overview
Internal implementation function for converting string input to float8 (double precision) values, providing platform-independent parsing with advanced error handling and support for special floating-point values.

## Definition

```c
float8
float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string,
				  struct Node *escontext)
```
## Detailed Description
This function serves as the core implementation for parsing string representations of double-precision floating-point numbers. It extends standard C library  functionality with PostgreSQL-specific error handling, whitespace management, and support for special values like NaN and Infinity. The function is designed to be reusable across different PostgreSQL data types that need to parse floating-point substrings.

Key features include:
- Leading and trailing whitespace handling
- Platform-independent parsing of special values (NaN, Infinity, +/-Inf)
- Comprehensive error reporting with context information
- Support for soft error handling through ErrorSaveContext
- Range validation for denormalized numbers

## Parameters / Member Variables
- `*num`: Input string containing the number to parse (modifiable for whitespace skipping)
- `**endptr_p`: Optional pointer to receive the position where parsing stopped (NULL means require complete consumption)
- `*type_name`: Name of the calling data type for error messages (e.g., "double precision", "point")
- `*orig_string`: Original input string for error reporting (may be larger than the parsed substring)
- `*escontext`: Error context for soft error handling (NULL for normal error throwing)
## Dependencies
- Functions called/Symbols referenced:
  - ereturn (error handling with context support)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (case-insensitive string comparison)
  - [get_float8_nan](../g/get_float8_nan.md) (retrieve NaN value)
  - [get_float8_infinity](../g/get_float8_infinity.md) (retrieve positive infinity value)
  - strtod (standard C library function)
  - isspace (standard C library function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)

- Called from (representative examples):
  - [float8in](float8in.md) (main float8 input function)
  - [single_decode](../s/single_decode.md) (geometric operations)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution)

## Notes and Other Information
- Handles platform differences in strtod() behavior for special values
- Provides more robust error messages than standard strtod()
- Supports both strict parsing (when endptr_p is NULL) and partial parsing
- Special handling for denormalized numbers that might incorrectly trigger ERANGE
- Designed for reuse in composite types like point, box, etc. where floating-point values are parsed as substrings

## Simplified Source

```c
float8
float8in_internal(char *num, char **endptr_p, const char *type_name,
                  const char *orig_string, struct Node *escontext)
{
    double val;
    char *endptr;

    // Skip leading whitespace
    while (*num != '\0' && isspace((unsigned char) *num))
        num++;

    // Check for empty string
    if (*num == '\0')
        ereturn(escontext, 0, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type %s: \"%s\"", type_name, orig_string)));

    // Try standard parsing first
    errno = 0;
    val = strtod(num, &endptr);

    // Handle parsing failures and special values
    if (endptr == num || errno != 0)
    {
        // Check for special floating-point values
        if (pg_strncasecmp(num, "NaN", 3) == 0)
        {
            val = get_float8_nan();
            endptr = num + 3;
        }
        else if (pg_strncasecmp(num, "Infinity", 8) == 0)
        {
            val = get_float8_infinity();
            endptr = num + 8;
        }
        else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
        {
            val = get_float8_infinity();
            endptr = num + 9;
        }
        else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
        {
            val = -get_float8_infinity();
            endptr = num + 9;
        }
        // Similar handling for "inf" variants...
        else if (errno == ERANGE && (val == 0.0 || val >= HUGE_VAL || val <= -HUGE_VAL))
        {
            // Value is out of range
            char *errnumber = pstrdup(num);
            errnumber[endptr - num] = '\0';
            ereturn(escontext, 0, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("\"%s\" is out of range for type double precision", errnumber)));
        }
        else
        {
            // Invalid syntax
            ereturn(escontext, 0, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s: \"%s\"", type_name, orig_string)));
        }
    }

    // Skip trailing whitespace
    while (*endptr != '\0' && isspace((unsigned char) *endptr))
        endptr++;

    // Set end pointer or validate complete consumption
    if (endptr_p)
        *endptr_p = endptr;
    else if (*endptr != '\0')
        ereturn(escontext, 0, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type %s: \"%s\"", type_name, orig_string)));

    return val;
}
```