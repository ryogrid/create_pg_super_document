# RE_wchar_execute

## Location
src/backend/utils/adt/regexp.c: 282 - 323

## Overview
Executes a compiled regular expression against wide character (pg_wchar) data, serving as the core matching engine for PostgreSQL's regex operations.

## Definition
```c
static bool RE_wchar_execute(regex_t *re, pg_wchar *data, int data_len, int start_search, int nmatch, regmatch_t *pmatch)
```

## Detailed Description
This function provides the low-level regex execution capability for PostgreSQL by interfacing with Spencer's regex library. It takes a pre-compiled regular expression and matches it against an array of wide characters (pg_wchar). The function handles the actual pattern matching logic and can optionally capture match details including subgroup positions. It performs error handling for regex execution failures and translates them into appropriate PostgreSQL errors.

The function is designed to work with PostgreSQL's internal wide character representation, which allows proper handling of multibyte characters in various database encodings. It supports partial matching starting from a specified offset within the data.

## Parameters / Member Variables
- `re`: Pointer to a compiled regular expression (regex_t) from RE_compile_and_cache
- `data`: Array of wide characters to search within (need not be null-terminated)
- `data_len`: Length of the data array in pg_wchar units
- `start_search`: Offset within data where searching should begin
- `nmatch`: Number of match result slots available in pmatch array
- `pmatch`: Optional array to store match positions for captured groups

## Dependencies
- Functions called/Symbols referenced:
  - pg_regexec (core regex execution from Spencer's library)
  - [pg_regerror](../p/pg_regerror.md) (error message generation)
  - regex_t, regmatch_t (data structures)
  - REG_OKAY, REG_NOMATCH (result constants)
- Called from (representative examples):
  - [RE_execute](RE_execute.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)

## Notes and Other Information
- This is a static function, only used internally within regexp.c
- Returns boolean result: true for match, false for no match
- Handles regex execution errors by throwing PostgreSQL ERRORs
- Works with Spencer's regex library which requires wide character input
- The pmatch parameter can be NULL if match position details are not needed
- Supports starting searches at arbitrary positions within the data for incremental matching