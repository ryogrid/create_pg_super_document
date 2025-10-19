# pg_strtoint16_safe

## Location
[src/backend/utils/adt/numutils.c:127-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L127-L382)

## Overview
Safely converts a string representation of a number to a signed 16-bit integer with comprehensive error handling and support for multiple number bases.

## Definition
```c
int16 pg_strtoint16_safe(const char *s, Node *escontext)
```

## Detailed Description
This function provides robust string-to-integer conversion for 16-bit signed integers with advanced error handling capabilities. It implements a two-path parsing strategy: a fast path optimized for common base-10 numbers without separators, and a comprehensive slow path that handles all supported formats including hexadecimal (0x/0X), octal (0o/0O), binary (0b/0B), and underscore digit separators.

The function uses unsigned arithmetic internally to properly handle two's complement representation, particularly for the most negative 16-bit value. It supports flexible input formatting including leading/trailing whitespace, optional sign characters, and underscore separators between digits for improved readability.

Error handling is performed through PostgreSQL's `ereturn()` mechanism, which allows errors to be either thrown immediately or captured in an ErrorSaveContext for later processing, depending on the `escontext` parameter.

## Parameters / Member Variables
- `s`: A null-terminated string containing the number to convert, supporting:
- `escontext`: Error context node for handling conversion errors; if NULL, errors are thrown via `ereport()`

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction hint)
  - PG_INT16_MIN (minimum 16-bit signed integer constant)
  - PG_INT16_MAX (maximum 16-bit signed integer constant)
  - ereturn (error return mechanism)
- Called from (representative examples):
  - [int2in](../i/int2in.md)
  - [pg_strtoint16](pg_strtoint16.md)

## Notes and Other Information
- Implements a performance-optimized two-path parsing strategy: fast path for simple decimal numbers, slow path for complex formats
- Uses unsigned arithmetic accumulation to handle two's complement edge cases correctly
- Supports hexadecimal digits via `hexlookup` table for efficient conversion
- Validates underscore placement rules: not at the beginning/end, must be between valid digits
- Returns proper PostgreSQL error codes: ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for overflow, ERRCODE_INVALID_TEXT_REPRESENTATION for syntax errors
- The function name follows PostgreSQL's `_safe` convention for functions that use ErrorSaveContext
- Handles the full range of 16-bit signed integers (-32,768 to 32,767)
- Branch prediction hints (`likely`/`unlikely`) optimize performance for common cases

## Simplified Source

```c
int16 pg_strtoint16_safe(const char *s, Node *escontext) {
    const char *ptr = s;
    uint16 tmp = 0;
    bool neg = false;

    // Fast path: try simple decimal parsing first
    if (*ptr == '-') {
        ptr++;
        neg = true;
    }

    // Parse digits quickly for common case
    unsigned char digit = (*ptr - '0');
    if (digit < 10) {
        ptr++;
        tmp = digit;

        // Continue parsing decimal digits
        while ((digit = (*ptr - '0')) < 10) {
            ptr++;
            if (tmp > -(PG_INT16_MIN / 10))
                goto out_of_range;
            tmp = tmp * 10 + digit;
        }

        // If we consumed the whole string, we're done
        if (*ptr == '\0') {
            if (neg) {
                if (tmp > (uint16)(-(PG_INT16_MIN + 1)) + 1)
                    goto out_of_range;
                return -((int16)tmp);
            }
            if (tmp > PG_INT16_MAX)
                goto out_of_range;
            return (int16)tmp;
        }
    }

    // Slow path: handle spaces, different bases, underscores
    tmp = 0;
    ptr = s;

    // Skip leading whitespace
    while (isspace(*ptr)) ptr++;

    // Handle sign
    if (*ptr == '-') {
        ptr++;
        neg = true;
    } else if (*ptr == '+') {
        ptr++;
    }

    // Determine base and parse accordingly
    if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X')) {
        // Hexadecimal
        ptr += 2;
        while (isxdigit(*ptr) || *ptr == '_') {
            if (*ptr == '_') {
                ptr++;
                if (!isxdigit(*ptr)) goto invalid_syntax;
                continue;
            }
            if (tmp > -(PG_INT16_MIN / 16)) goto out_of_range;
            tmp = tmp * 16 + hexlookup[*ptr++];
        }
    } else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O')) {
        // Octal
        ptr += 2;
        while ((*ptr >= '0' && *ptr <= '7') || *ptr == '_') {
            if (*ptr == '_') {
                ptr++;
                if (*ptr < '0' || *ptr > '7') goto invalid_syntax;
                continue;
            }
            if (tmp > -(PG_INT16_MIN / 8)) goto out_of_range;
            tmp = tmp * 8 + (*ptr++ - '0');
        }
    } else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B')) {
        // Binary
        ptr += 2;
        while ((*ptr >= '0' && *ptr <= '1') || *ptr == '_') {
            if (*ptr == '_') {
                ptr++;
                if (*ptr < '0' || *ptr > '1') goto invalid_syntax;
                continue;
            }
            if (tmp > -(PG_INT16_MIN / 2)) goto out_of_range;
            tmp = tmp * 2 + (*ptr++ - '0');
        }
    } else {
        // Decimal with possible underscores
        while ((*ptr >= '0' && *ptr <= '9') || *ptr == '_') {
            if (*ptr == '_') {
                ptr++;
                if (!isdigit(*ptr)) goto invalid_syntax;
                continue;
            }
            if (tmp > -(PG_INT16_MIN / 10)) goto out_of_range;
            tmp = tmp * 10 + (*ptr++ - '0');
        }
    }

    // Skip trailing whitespace
    while (isspace(*ptr)) ptr++;

    if (*ptr != '\0') goto invalid_syntax;

    // Convert to signed result
    if (neg) {
        if (tmp > (uint16)(-(PG_INT16_MIN + 1)) + 1)
            goto out_of_range;
        return -((int16)tmp);
    }

    if (tmp > PG_INT16_MAX)
        goto out_of_range;
    return (int16)tmp;

out_of_range:
    ereturn(escontext, 0,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("value \"%s\" is out of range for type %s", s, "smallint")));

invalid_syntax:
    ereturn(escontext, 0,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"", "smallint", s)));
}
```