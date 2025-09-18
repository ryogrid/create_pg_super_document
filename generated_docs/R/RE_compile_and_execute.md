# RE_compile_and_execute

## Location
src/backend/utils/adt/regexp.c: 358 - 384

## Overview
Provides a high-level interface for compiling and executing regular expressions in a single operation, combining caching and pattern matching for PostgreSQL's regex operations.

## Definition
```c
bool RE_compile_and_execute(text *text_re, char *dat, int dat_len, int cflags, Oid collation, int nmatch, regmatch_t *pmatch)
```

## Detailed Description
This function serves as the primary public interface for PostgreSQL's regular expression matching operations. It combines the compilation and execution phases into a single convenient function call. The function leverages the caching provided by RE_compile_and_cache to avoid repeated compilation of the same patterns, and then delegates the actual matching to RE_execute.

The function includes an optimization where it automatically sets the REG_NOSUB flag when the caller doesn't need subexpression match details (nmatch < 2), which can improve performance by avoiding unnecessary capture group processing in the underlying regex engine.

## Parameters / Member Variables
- `text_re`: The regular expression pattern as a TEXT object in database encoding
- `dat`: Input data string to match against (need not be null-terminated)
- `dat_len`: Length of the input data string in bytes
- `cflags`: Compilation flags controlling regex behavior (case sensitivity, etc.)
- `collation`: Collation OID to use for LC_CTYPE-dependent behavior
- `nmatch`: Number of match result slots available in pmatch array
- `pmatch`: Optional array to store match positions for captured groups

## Dependencies
- Functions called/Symbols referenced:
  - [RE_compile_and_cache](RE_compile_and_cache.md) (pattern compilation and caching)
  - [RE_execute](RE_execute.md) (pattern execution with encoding conversion)
  - regex_t, regmatch_t (data structures)
  - REG_NOSUB (compilation flag for optimization)
- Called from (representative examples):
  - [nameregexeq](../n/nameregexeq.md), nameregexne (name pattern matching functions)
  - [textregexeq](../t/textregexeq.md), textregexne (text pattern matching functions)
  - [nameicregexeq](../n/nameicregexeq.md), nameicregexne (case-insensitive name matching)
  - [texticregexeq](../t/texticregexeq.md), texticregexne (case-insensitive text matching)
  - [regexp_like](../r/regexp_like.md) (SQL REGEXP_LIKE function)
  - [executeLikeRegex](../e/executeLikeRegex.md) (JSON path regex execution)

## Notes and Other Information
- This is the main public interface for regex operations in PostgreSQL
- Automatically optimizes performance by setting REG_NOSUB when submatches are not needed
- Benefits from the LRU caching implemented in RE_compile_and_cache
- Returns boolean result: true for match, false for no match
- Handles both pattern compilation and execution errors through the underlying functions
- Widely used throughout PostgreSQL for various regex operations including SQL operators and JSON path expressions