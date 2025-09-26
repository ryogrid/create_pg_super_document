# from_char_parse_int

## Location
src/backend/utils/adt/formatting.c: 2560 - 2577

## Overview
A convenience wrapper function that parses integers from strings using the format keyword length as the expected field length.

## Definition
```c
static int from_char_parse_int(int *dest, const char **src, FormatNode *node, Node *escontext)
```

## Detailed Description
This function provides a simplified interface to from_char_parse_int_len() for common cases where the expected field length matches the format keyword length. It automatically extracts the length from the FormatNode's key and delegates the actual parsing work to from_char_parse_int_len(). This wrapper is used for standard date/time format patterns like "DD", "MM", "YY" where the keyword length corresponds directly to the expected input field width.

## Parameters / Member Variables
- `dest`: Pointer to destination integer where the parsed value will be stored
- `src`: Pointer to source string pointer (advanced after parsing)
- `node`: Pointer to FormatNode containing the format specification and key information
- `escontext`: Node pointer for error context handling, enables soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - from_char_parse_int_len (core parsing function)
  - FormatNode (struct type)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1062)
  - DCH_from_char (multiple locations: 3572, 3577, 3618, 3759, 3788, 3798, 3803, 3817, 3833, 3838, 3864, 3871, 3881, 3891, 3910, 3915)

## Notes and Other Information
- Returns the number of characters consumed on success, -1 on error
- Should not be used for format keywords where field length differs from keyword length (e.g., HH24 has keyword length 4 but field length 2)
- For cases with mismatched lengths, call from_char_parse_int_len() directly with explicit length
- Provides a cleaner, more maintainable interface for the majority of date/time parsing scenarios
- Part of the layered design of PostgreSQL's formatting system, offering both convenience and flexibility