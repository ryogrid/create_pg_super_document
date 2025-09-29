# pg_strtoint64_safe

## Location
[src/backend/utils/adt/numutils.c:651-899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L651-L899)

## Overview
Converts a string representation of an integer to a signed 64-bit integer value with comprehensive error handling, supporting multiple number bases and underscore separators.

## Definition

```c
int64
pg_strtoint64_safe(const char *s, Node *escontext)
```
## Detailed Description
This function provides robust string-to-64-bit-integer conversion with comprehensive error handling and format support. Like its 32-bit counterpart, it implements a two-phase parsing strategy with a fast path for simple base-10 numbers and a comprehensive slow path for complex formats. It supports hexadecimal (0x/0X), octal (0o/0O), binary (0b/0B), and decimal number formats, along with underscore separators between digits for improved readability.

The function uses unsigned arithmetic internally to correctly handle the full range of 64-bit signed integers, including proper handling of the most negative value in two's complement representation. It provides both hard error (ereport) and soft error (ErrorSaveContext) handling modes.

## Parameters / Member Variables
- : Input string containing the integer representation to convert  
- : Error context node for soft error handling; if NULL, errors are thrown via ereport()

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction macro)
  - PG_INT64_MIN (minimum 64-bit signed integer constant)
  - PG_INT64_MAX (maximum 64-bit signed integer constant)  
  - ereturn (error return macro for soft error handling)
- Called from (representative examples):
  - [make_const](../m/make_const.md) (parser node creation function)
  - [int8in](../i/int8in.md) (bigint input function)
  - [pg_strtoint64](pg_strtoint64.md) (wrapper function without error context)

## Notes and Other Information
- Uses fast path optimization for common base-10 integers without special formatting
- Supports comprehensive number format parsing: decimal, hex (0x), octal (0o), binary (0b)
- Allows underscore digit separators for readability (e.g., 1_000_000_000)
- Handles leading and trailing whitespace gracefully
- Provides both hard and soft error handling through ErrorSaveContext
- Uses unsigned arithmetic internally for correct two's complement edge case handling
- Essential component of PostgreSQL's bigint type input processing
- Accumulates values as unsigned to handle the full signed range correctly

## Simplified Source

```c
int64 pg_strtoint64_safe(const char *s, Node *escontext)
{
    const char *ptr = s;
    const char *firstdigit;
    uint64 tmp = 0;
    bool neg = false;
    unsigned char digit;

    // Fast path: try to parse simple decimal numbers quickly
    if (*ptr == '-')
    {
        ptr++;
        neg = true;
    }

    // Process first digit
    digit = (*ptr - '0');
    if (likely(digit < 10))
    {
        ptr++;
        tmp = digit;
    }
    else
        goto slow;  // Need at least one digit

    // Process remaining digits in fast path
    for (;;)
    {
        digit = (*ptr - '0');
        if (digit >= 10)
            break;
        ptr++;

        if (unlikely(tmp > -(PG_INT64_MIN / 10)))
            goto out_of_range;
        tmp = tmp * 10 + digit;
    }

    // If string doesn't end cleanly, use slow path
    if (unlikely(*ptr != '\0'))
        goto slow;

    // Convert to final result with range checking
    if (neg)
    {
        if (unlikely(tmp > (uint64) (-(PG_INT64_MIN + 1)) + 1))
            goto out_of_range;
        return -((int64) tmp);
    }

    if (unlikely(tmp > PG_INT64_MAX))
        goto out_of_range;
    return (int64) tmp;

slow:
    // Slow path: handle complex formats (hex, octal, binary, underscores)
    tmp = 0;
    ptr = s;

    // Skip leading spaces
    while (isspace((unsigned char) *ptr))
        ptr++;

    // Handle sign
    if (*ptr == '-')
    {
        ptr++;
        neg = true;
    }
    else if (*ptr == '+')
        ptr++;

    // Parse based on prefix
    if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
    {
        // Hexadecimal: 0x...
        firstdigit = ptr += 2;
        for (;;)
        {
            if (isxdigit((unsigned char) *ptr))
            {
                if (unlikely(tmp > -(PG_INT64_MIN / 16)))
                    goto out_of_range;
                tmp = tmp * 16 + hexlookup[(unsigned char) *ptr++];
            }
            else if (*ptr == '_')
            {
                ptr++;
                if (*ptr == '\0' || !isxdigit((unsigned char) *ptr))
                    goto invalid_syntax;
            }
            else
                break;
        }
    }
    else if (ptr[0] == '0' && (ptr[1] == 'o' || ptr[1] == 'O'))
    {
        // Octal: 0o...
        firstdigit = ptr += 2;
        for (;;)
        {
            if (*ptr >= '0' && *ptr <= '7')
            {
                if (unlikely(tmp > -(PG_INT64_MIN / 8)))
                    goto out_of_range;
                tmp = tmp * 8 + (*ptr++ - '0');
            }
            else if (*ptr == '_')
            {
                ptr++;
                if (*ptr == '\0' || *ptr < '0' || *ptr > '7')
                    goto invalid_syntax;
            }
            else
                break;
        }
    }
    else if (ptr[0] == '0' && (ptr[1] == 'b' || ptr[1] == 'B'))
    {
        // Binary: 0b...
        firstdigit = ptr += 2;
        for (;;)
        {
            if (*ptr >= '0' && *ptr <= '1')
            {
                if (unlikely(tmp > -(PG_INT64_MIN / 2)))
                    goto out_of_range;
                tmp = tmp * 2 + (*ptr++ - '0');
            }
            else if (*ptr == '_')
            {
                ptr++;
                if (*ptr == '\0' || *ptr < '0' || *ptr > '1')
                    goto invalid_syntax;
            }
            else
                break;
        }
    }
    else
    {
        // Decimal with underscores
        firstdigit = ptr;
        for (;;)
        {
            if (*ptr >= '0' && *ptr <= '9')
            {
                if (unlikely(tmp > -(PG_INT64_MIN / 10)))
                    goto out_of_range;
                tmp = tmp * 10 + (*ptr++ - '0');
            }
            else if (*ptr == '_')
            {
                if (unlikely(ptr == firstdigit))
                    goto invalid_syntax;
                ptr++;
                if (*ptr == '\0' || !isdigit((unsigned char) *ptr))
                    goto invalid_syntax;
            }
            else
                break;
        }
    }

    // Require at least one digit
    if (unlikely(ptr == firstdigit))
        goto invalid_syntax;

    // Allow trailing whitespace
    while (isspace((unsigned char) *ptr))
        ptr++;

    if (unlikely(*ptr != '\0'))
        goto invalid_syntax;

    // Final conversion with range checking
    if (neg)
    {
        if (tmp > (uint64) (-(PG_INT64_MIN + 1)) + 1)
            goto out_of_range;
        return -((int64) tmp);
    }

    if (tmp > PG_INT64_MAX)
        goto out_of_range;
    return (int64) tmp;

out_of_range:
    ereturn(escontext, 0,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("value \"%s\" is out of range for type %s", s, "bigint")));

invalid_syntax:
    ereturn(escontext, 0,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"", "bigint", s)));
}
```