# from_char_parse_int_len

## Location
[src/backend/utils/adt/formatting.c:2463-2559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2463-L2559)

## Overview
A comprehensive function that parses integer values from strings during date/time formatting operations, with support for both fixed-width and flexible parsing modes.

## Definition
```c
static int from_char_parse_int_len(int *dest, const char **src, const int len, FormatNode *node, Node *escontext)
```

## Detailed Description
This function is a core component of PostgreSQL's date/time parsing infrastructure that extracts integer values from input strings according to formatting specifications. It supports two parsing modes: fixed-width mode (default) where exactly 'len' characters are consumed, and Fill Mode (FM) where it consumes as many digits as available. The function handles whitespace skipping, validates field lengths, performs range checking, and integrates with PostgreSQL's soft error handling mechanism.

## Parameters / Member Variables
- `dest`: Pointer to destination integer (can be NULL to discard result)
- `src`: Pointer to source string pointer (advanced after parsing)
- `len`: Maximum number of characters to consume in fixed-width mode
- `node`: Pointer to FormatNode containing formatting context and specifications
- `escontext`: Node pointer for error context handling, enables soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [strspace_len](../s/strspace_len.md) (whitespace counting utility)
  - [strlcpy](../s/strlcpy.md) (safe string copying)
  - strtol (string to long conversion)
  - S_FM (Fill Mode suffix check macro)
  - [is_next_separator](../i/is_next_separator.md) (separator detection utility)
  - [from_char_set_int](from_char_set_int.md) (safe integer assignment)
  - ereturn (PostgreSQL error handling macro)
  - DCH_MAX_ITEM_SIZ (maximum item size constant)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1060)
  - [from_char_parse_int](from_char_parse_int.md) (formatting.c:2563)
  - [DCH_from_char](../D/DCH_from_char.md) (multiple locations: 3561, 3567, 3582, 3603, 3672, 3677, 3704, 3711, 3793, 3808)

## Notes and Other Information
- Returns the number of characters consumed on success, -1 on error
- Automatically skips leading whitespace (not counted against length limit)
- In Fill Mode (FM), parses all available digits regardless of length specification
- Provides detailed error messages for various failure conditions (too short, invalid format, out of range)
- Performs range validation ensuring values fit within INT_MIN to INT_MAX
- Integrates with from_char_set_int for conflict detection during field assignment
- Critical for robust parsing of numeric date/time components like days, months, years, etc.

## Simplified Source

```c
static int
from_char_parse_int_len(int *dest, const char **src, const int len, FormatNode *node, Node *escontext)
{
    long result;
    char copy[DCH_MAX_ITEM_SIZ + 1];
    const char *init = *src;
    int used;

    // Skip leading whitespace
    *src += strspace_len(*src);

    // Copy up to 'len' characters for parsing
    used = (int) strlcpy(copy, *src, len + 1);

    if (S_FM(node->suffix) || is_next_separator(node)) {
        // Fill Mode: parse as many digits as available
        char *endptr;
        errno = 0;
        result = strtol(init, &endptr, 10);
        *src = endptr;
    } else {
        // Fixed-width mode: parse exactly 'len' characters
        char *last;

        if (used < len)
            ereturn(escontext, -1, /* error: source too short */);

        errno = 0;
        result = strtol(copy, &last, 10);
        used = last - copy;

        if (used > 0 && used < len)
            ereturn(escontext, -1, /* error: partial parse */);

        *src += used;
    }

    // Validate that we parsed something
    if (*src == init)
        ereturn(escontext, -1, /* error: no digits found */);

    // Range check
    if (errno == ERANGE || result < INT_MIN || result > INT_MAX)
        ereturn(escontext, -1, /* error: value out of range */);

    // Store result if destination provided
    if (dest != NULL) {
        if (!from_char_set_int(dest, (int) result, node, escontext))
            return -1;
    }

    return *src - init;
}
```