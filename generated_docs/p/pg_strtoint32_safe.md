# pg_strtoint32_safe

## Location
[src/backend/utils/adt/numutils.c:389-644](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L389-L644)

## Overview
Converts a string representation of an integer to a signed 32-bit integer value with error handling support, supporting multiple number bases and underscore separators for readability.

## Definition

```c
int32
pg_strtoint32_safe(const char *s, Node *escontext)
```
## Detailed Description
This function provides a robust string-to-integer conversion with comprehensive error handling. It implements a two-phase parsing strategy: a fast path for simple base-10 numbers and a slower comprehensive path that handles hexadecimal (0x/0X), octal (0o/0O), binary (0b/0B), and decimal formats. The function supports underscore separators between digits for improved readability and handles both positive and negative numbers with proper overflow detection.

The function uses unsigned arithmetic internally to correctly handle the full range of 32-bit signed integers, including the most negative value that cannot be represented as a positive number in two's complement representation.

## Parameters / Member Variables
- : Input string containing the integer representation to convert
- : Error context node for soft error handling; if NULL, errors are thrown via ereport()

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction macro)
  - PG_INT32_MIN (minimum 32-bit signed integer constant)
  - PG_INT32_MAX (maximum 32-bit signed integer constant)
  - ereturn (error return macro for soft error handling)
- Called from (representative examples):
  - [int4in](../i/int4in.md) (integer input function)
  - [pg_strtoint32](pg_strtoint32.md) (wrapper function without error context)

## Notes and Other Information
- Uses a fast path optimization for common base-10 integers without underscores
- Supports multiple number bases: decimal, hexadecimal (0x), octal (0o), and binary (0b)
- Allows underscore separators between digits for readability (e.g., 1_000_000)
- Handles leading and trailing whitespace
- Provides soft error handling through ErrorSaveContext when escontext is provided
- Uses unsigned arithmetic internally to handle two's complement edge cases correctly
- Part of PostgreSQL's robust numeric input parsing infrastructure

## Simplified Source

```c
int32
pg_strtoint32_safe(const char *s, Node *escontext)
{
    const char *ptr = s;
    uint32 tmp = 0;
    bool neg = false;

    // Fast path: handle simple base-10 numbers
    if (*ptr == '-') {
        ptr++;
        neg = true;
    }

    // Parse decimal digits
    if ((*ptr - '0') < 10) {
        tmp = (*ptr++ - '0');

        while ((*ptr - '0') < 10) {
            if (tmp > -(PG_INT32_MIN / 10))
                goto out_of_range;
            tmp = tmp * 10 + (*ptr++ - '0');
        }

        if (*ptr == '\0') {
            // Fast path complete
            return neg ? -((int32) tmp) : (int32) tmp;
        }
    }

    // Slow path: handle hex (0x), octal (0o), binary (0b), and underscores
    // ... [detailed parsing for different bases] ...

    // Final range checking and return
    if (neg) {
        if (tmp > (uint32)(-(PG_INT32_MIN + 1)) + 1)
            goto out_of_range;
        return -((int32) tmp);
    }

    if (tmp > PG_INT32_MAX)
        goto out_of_range;

    return (int32) tmp;

out_of_range:
    ereturn(escontext, 0,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("value \"%s\" is out of range for type integer", s)));
}
```