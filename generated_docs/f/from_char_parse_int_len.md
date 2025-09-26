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
  - strspace_len (whitespace counting utility)
  - strlcpy (safe string copying)
  - strtol (string to long conversion)
  - S_FM (Fill Mode suffix check macro)
  - is_next_separator (separator detection utility)
  - from_char_set_int (safe integer assignment)
  - ereturn (PostgreSQL error handling macro)
  - DCH_MAX_ITEM_SIZ (maximum item size constant)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1060)
  - from_char_parse_int (formatting.c:2563)
  - DCH_from_char (multiple locations: 3561, 3567, 3582, 3603, 3672, 3677, 3704, 3711, 3793, 3808)

## Notes and Other Information
- Returns the number of characters consumed on success, -1 on error
- Automatically skips leading whitespace (not counted against length limit)
- In Fill Mode (FM), parses all available digits regardless of length specification
- Provides detailed error messages for various failure conditions (too short, invalid format, out of range)
- Performs range validation ensuring values fit within INT_MIN to INT_MAX
- Integrates with from_char_set_int for conflict detection during field assignment
- Critical for robust parsing of numeric date/time components like days, months, years, etc.