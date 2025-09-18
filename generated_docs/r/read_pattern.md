# read_pattern

## Location
src/bin/pg_dump/filter.c: 303 - 392

## Overview
Parses object identifier patterns from filter input, handling both quoted and unquoted identifiers while sanitizing whitespace and formatting.

## Definition
```c
static const char *read_pattern(FilterStateData *fstate, const char *str, PQExpBuffer pattern)
```

## Detailed Description
This function parses valid PostgreSQL object identifiers from input text, which can be quoted or unquoted, qualified or unqualified, and may include full signatures for routines. It takes special care to sanitize the detected identifier by removing extraneous whitespaces and formatting it consistently. This normalization is crucial because most backup/restore filtering functions only recognize identifiers when they match the exact format output by the PostgreSQL server. The function handles various special characters including dots (.), parentheses (), commas (,), and double quotes (").

## Parameters / Member Variables
- `fstate`: Pointer to FilterStateData containing parsing state and error handling context
- `str`: Pointer to the current position in the input string to parse from
- `pattern`: PQExpBuffer to append the parsed and normalized pattern to

## Dependencies
- Functions called/Symbols referenced:
  - [pg_log_filter_error](../p/pg_log_filter_error.md)
  - [exit_nicely](../e/exit_nicely.md) (via fstate function pointer)
  - appendPQExpBufferChar
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [read_quoted_string](read_quoted_string.md)
  - isspace (standard C library)
  - strchr (standard C library)
  - FilterStateData (struct type)
- Called from (representative examples):
  - [filter_read_item](../f/filter_read_item.md) (at src/bin/pg_dump/filter.c:456)

## Notes and Other Information
- This is a static function, only accessible within the filter.c file
- Handles complex identifier parsing including schema-qualified names and function signatures
- Normalizes whitespace by removing unnecessary spaces while preserving required spacing
- Stops parsing when encountering '#' character (comment start) or end of string
- Supports quoted identifiers by delegating to read_quoted_string function
- Part of pg_dump's object filtering mechanism for selective database dumps
- Will terminate the program via exit_nicely if no object name pattern is found
- Special handling for punctuation characters: commas get formatted with trailing space, dots and parentheses are preserved as-is