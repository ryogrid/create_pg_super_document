# textregexreplace_noopt

## Location
[src/backend/utils/adt/regexp.c:642-657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L642-L657)

## Overview
A PostgreSQL function that performs regular expression-based text replacement with default options (case-sensitive, replace first occurrence only).

## Definition
```c
Datum textregexreplace_noopt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `textregexreplace_noopt` function implements a simplified version of PostgreSQL's regular expression replacement functionality. It takes a source text, a regular expression pattern, and a replacement string, then returns a new text value with the first occurrence of the pattern replaced by the replacement string. This function uses default options: case-sensitive matching and replaces only the first match. It serves as a convenience wrapper around the more complex `replace_text_regexp` function with predefined parameters.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: The source text string to search and replace within
- `PG_GETARG_TEXT_PP(1)`: The regular expression pattern to match
- `PG_GETARG_TEXT_PP(2)`: The replacement string to substitute for matches

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`: Extracts text arguments with potential detoasting
  - `[replace_text_regexp](../r/replace_text_regexp.md)`: Core function that performs the actual regex replacement
  - `PG_RETURN_TEXT_P`: Returns a text value from the function
  - `PG_GET_COLLATION`: Gets collation information for the operation
  - `REG_ADVANCED`: Flag for advanced regular expression features
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Uses default replacement options: case-sensitive matching, first occurrence only
- Serves as a simplified interface to the more complex regex replacement functionality
- The function parameters to replace_text_regexp are: (text, pattern, replacement, flags, collation, start_search, max_replacements)
- Uses 0 for start_search (beginning of string) and 1 for max_replacements (first match only)
- Part of PostgreSQL's comprehensive regular expression replacement functionality
- Typically called through SQL regexp_replace() function without options parameter

## Simplified Source

```c
Datum textregexreplace_noopt(PG_FUNCTION_ARGS) {
    text *source = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);
    text *replacement = PG_GETARG_TEXT_PP(2);

    // Perform regex replacement with default options
    // Case-sensitive, replace first match only
    PG_RETURN_TEXT_P(replace_text_regexp(source, pattern, replacement,
                                        REG_ADVANCED, PG_GET_COLLATION(),
                                        0, 1));
}
```