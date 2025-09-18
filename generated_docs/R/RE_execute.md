# RE_execute

## Location
src/backend/utils/adt/regexp.c: 324 - 357

## Overview
Executes a compiled regular expression against data in database encoding, providing character encoding conversion and delegating to the core wide-character matching engine.

## Definition
```c
static bool RE_execute(regex_t *re, char *dat, int dat_len, int nmatch, regmatch_t *pmatch)
```

## Detailed Description
This function serves as a wrapper around RE_wchar_execute, handling the character encoding conversion necessary for PostgreSQL's regex operations. It takes input data in the database encoding (typically UTF-8 or another multibyte encoding) and converts it to the wide character format (pg_wchar) required by Spencer's regex library. After conversion, it delegates the actual pattern matching to RE_wchar_execute and returns the result.

The function manages memory allocation for the character conversion process, ensuring proper cleanup after execution. It always starts matching from the beginning of the data (offset 0) and is designed for simpler use cases where full string matching is needed.

## Parameters / Member Variables
- `re`: Pointer to a compiled regular expression (regex_t) from RE_compile_and_cache
- `dat`: Input data string in database encoding (need not be null-terminated)
- `dat_len`: Length of the input data string in bytes
- `nmatch`: Number of match result slots available in pmatch array
- `pmatch`: Optional array to store match positions for captured groups

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (character encoding conversion)
  - [RE_wchar_execute](RE_wchar_execute.md) (core wide-character regex execution)
  - [palloc](../p/palloc.md), pfree (memory management)
  - regex_t, regmatch_t (data structures)
- Called from (representative examples):
  - [RE_compile_and_execute](RE_compile_and_execute.md)
  - [textregexsubstr](../t/textregexsubstr.md)

## Notes and Other Information
- This is a static function, only used internally within regexp.c
- Acts as an encoding conversion layer between database strings and wide-character regex engine
- Always starts matching from position 0 (beginning of string)
- Handles memory allocation and cleanup for the character conversion process
- Returns boolean result: true for match, false for no match
- The converted wide character data is automatically freed after matching