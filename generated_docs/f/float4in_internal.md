# float4in_internal

## Location
[src/backend/utils/adt/float.c:176-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L176-L311)

## Overview
Internal function that provides the core logic for parsing string representations into single-precision floating-point values (float4) with platform-independent behavior and comprehensive error handling.

## Definition

```c
float4
float4in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string,
				  struct Node *escontext)
```
## Detailed Description
The float4in_internal function serves as the robust core implementation for converting string input to float4 values. It provides a platform-independent way of parsing floating-point numbers with behavior similar to strtof() but with enhanced error handling through PostgreSQL's error context system. The function handles special values like NaN and Infinity, manages whitespace, and provides detailed error reporting for invalid inputs or out-of-range values.

The function uses strtof() as the primary parsing mechanism but supplements it with custom handling for special floating-point values (NaN, Infinity, +/-Inf) to ensure consistent behavior across different platforms. It also includes special logic for handling denormalized numbers and provides clear error messages for various failure cases.

## Parameters / Member Variables
- `*num`: Input string containing the number to be parsed
- `**endptr_p`: Optional pointer to store the position where parsing stopped (can be NULL)
- `*type_name`: Type name string used in error messages (e.g., "real")
- `*orig_string`: Original input string for error reporting purposes
- `*escontext`: Error context for soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - ereturn (error handling)
  - strtof (standard library float parsing)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (case-insensitive string comparison)
  - [get_float4_nan](../g/get_float4_nan.md) (NaN value generation)
  - get_float4_infinity (infinity value generation)
  - isinf (infinity check)
- Called from (representative examples):
  - [float4in](float4in.md)

## Notes and Other Information
- Designed to be platform-independent and handles various special cases that strtof() might not handle consistently
- Supports parsing of NaN, Infinity, +Infinity, -Infinity, Inf, +Inf, -Inf with case-insensitive matching
- Includes special handling for denormalized numbers and ERANGE errors
- Uses PostgreSQL's error context system (escontext) for soft error handling
- Skips leading and trailing whitespace automatically
- Provides detailed error messages with the problematic input string included

## Simplified Source

```c
float4 float4in_internal(char *num, char **endptr_p,
                        const char *type_name, const char *orig_string,
                        struct Node *escontext)
{
    float val;
    char *endptr;

    // Skip leading whitespace
    while (*num != '\0' && isspace((unsigned char) *num))
        num++;

    // Check for empty string
    if (*num == '\0')
        ereturn(escontext, 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        type_name, orig_string)));

    // Parse using strtof
    errno = 0;
    val = strtof(num, &endptr);

    // Handle parsing failures and special values
    if (endptr == num || errno != 0) {
        int save_errno = errno;

        // Try platform-independent special value parsing
        if (pg_strncasecmp(num, "NaN", 3) == 0) {
            val = get_float4_nan();
            endptr = num + 3;
        } else if (pg_strncasecmp(num, "Infinity", 8) == 0) {
            val = get_float4_infinity();
            endptr = num + 8;
        } else if (pg_strncasecmp(num, "+Infinity", 9) == 0) {
            val = get_float4_infinity();
            endptr = num + 9;
        } else if (pg_strncasecmp(num, "-Infinity", 9) == 0) {
            val = -get_float4_infinity();
            endptr = num + 9;
        } else if (pg_strncasecmp(num, "inf", 3) == 0) {
            val = get_float4_infinity();
            endptr = num + 3;
        } else if (pg_strncasecmp(num, "+inf", 4) == 0) {
            val = get_float4_infinity();
            endptr = num + 4;
        } else if (pg_strncasecmp(num, "-inf", 4) == 0) {
            val = -get_float4_infinity();
            endptr = num + 4;
        } else if (save_errno == ERANGE) {
            // Check for real overflow/underflow vs denormalized numbers
            if (val == 0.0 || isinf(val) || (val >= HUGE_VALF || val <= -HUGE_VALF)) {
                ereturn(escontext, 0,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("\"%.*s\" is out of range for type real",
                                (int)(endptr - num), num)));
            }
        } else {
            ereturn(escontext, 0,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s: \"%s\"",
                            type_name, orig_string)));
        }
    }

    // Skip trailing whitespace
    while (*endptr != '\0' && isspace((unsigned char) *endptr))
        endptr++;

    // Handle end pointer or validate complete consumption
    if (endptr_p)
        *endptr_p = endptr;
    else if (*endptr != '\0')
        ereturn(escontext, 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        type_name, orig_string)));

    return val;
}
```