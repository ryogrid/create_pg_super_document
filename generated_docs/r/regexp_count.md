# regexp_count

## Location
[src/backend/utils/adt/regexp.c:1092-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1092-L1134)

## Overview
A PostgreSQL SQL function that returns the number of matches of a regular expression pattern within a string.

## Definition
```c
Datum regexp_count(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL function `regexp_count(string, pattern [, start [, flags]])` that counts how many times a regular expression pattern matches within a given string. It supports optional parameters for specifying the starting position and regex flags to control matching behavior.

The function internally uses the `setup_regexp_matches()` function to perform the actual pattern matching with the global flag enabled, allowing it to find all matches in the string. The result is the total count of non-overlapping matches found.

Key behaviors:
- Starts searching from position 1 by default (can be overridden with start parameter)
- Uses PostgreSQL's built-in regex engine with POSIX extended regular expressions
- Supports various regex flags but explicitly prohibits the 'g' (global) flag from user specification
- Automatically enables global matching internally to count all matches

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `str` (arg 0): The target string to search within
  - `pattern` (arg 1): The regular expression pattern to match
  - `start` (arg 2, optional): Starting character position for search (1-based, defaults to 1)
  - `flags` (arg 3, optional): Regex flags string (e.g., 'i' for case-insensitive)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` (macro for extracting text arguments)
  - `PG_GETARG_TEXT_PP_IF_EXISTS` (macro for optional text arguments)
  - `PG_GETARG_INT32` (macro for extracting integer arguments)
  - `PG_NARGS` (macro for getting argument count)
  - [parse_re_flags](../p/parse_re_flags.md) (parses regex flags string)
  - [setup_regexp_matches](../s/setup_regexp_matches.md) (performs the actual regex matching)
  - `PG_GET_COLLATION` (gets current collation)
  - `PG_RETURN_INT32` (macro for returning integer result)
- Called from:
  - [regexp_count_no_start](regexp_count_no_start.md) (3-argument wrapper)
  - [regexp_count_no_flags](regexp_count_no_flags.md) (2-argument wrapper)
  - SQL queries using `regexp_count()` function

## Notes and Other Information
- Located in `src/backend/utils/adt/regexp.c:1092-1134`
- The start parameter must be positive (>= 1), following SQL standard 1-based indexing
- Users cannot specify the 'g' (global) flag directly as it's automatically enabled internally
- Returns 0 if no matches are found
- The function is strict with respect to NULL arguments (returns NULL if any required argument is NULL)
- Part of PostgreSQL's SQL standard regex functionality
- The function can handle multi-byte character encodings correctly

## Simplified Source

```c
Datum
regexp_count(PG_FUNCTION_ARGS)
{
    // Extract input parameters
    text *source_string = PG_GETARG_TEXT_PP(0);
    text *regex_pattern = PG_GETARG_TEXT_PP(1);
    int start_position = 1;  // Default start position
    text *flags = PG_GETARG_TEXT_PP_IF_EXISTS(3);
    pg_re_flags regex_flags;
    regexp_matches_ctx *match_context;

    // Handle optional start position parameter
    if (PG_NARGS() > 2) {
        start_position = PG_GETARG_INT32(2);
        if (start_position <= 0) {
            ereport(ERROR, "start position must be positive");
        }
    }

    // Parse regex flags from flags string
    parse_re_flags(&regex_flags, flags);

    // Validate that user didn't specify 'g' flag (we set it internally)
    if (regex_flags.glob) {
        ereport(ERROR, "regexp_count() does not support the \"global\" option");
    }

    // Enable global matching internally to find all matches
    regex_flags.glob = true;

    // Perform the regex matching to count occurrences
    // Convert 1-based start position to 0-based for internal use
    match_context = setup_regexp_matches(source_string, regex_pattern, &regex_flags,
                                       start_position - 1, PG_GET_COLLATION(),
                                       false,  // can ignore subexpressions
                                       false, false);

    // Return the total number of matches found
    PG_RETURN_INT32(match_context->nmatches);
}
```