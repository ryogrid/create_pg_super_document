# replace_text_regexp

## Location
[src/backend/utils/adt/varlena.c:4206-4367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4206-L4367)

## Overview
The core function that implements regular expression-based text replacement in PostgreSQL, supporting pattern matching with capture groups and sophisticated replacement text processing.

## Definition

```c
text *
replace_text_regexp(text *src_text, text *pattern_text,
					text *replace_text,
					int cflags, Oid collation,
					int search_start, int n)
```
## Detailed Description
This function performs regular expression search and replace operations on text strings. It supports advanced features including:

- **Pattern matching**: Uses POSIX regular expressions with configurable compilation flags
- **Capture groups**: Supports up to 9 numbered capture groups (\1-\9) plus the full match (\&)
- **Selective replacement**: Can replace all matches or just the N-th occurrence
- **Unicode support**: Properly handles multi-byte character encodings
- **Performance optimization**: Uses REG_NOSUB when replacement text contains no back-references

The function operates by converting the source text to wide characters for proper regex processing, then iteratively finding matches and building the result string by copying non-matching segments and processed replacement text.

## Parameters / Member Variables
- : Source text to search for pattern matches
- : Regular expression pattern to match against  
- : Replacement text that may contain back-references and escape sequences
- : Regular expression compilation flags (e.g., case sensitivity options)
- : Text collation to use for pattern matching
- : Character offset in src_text where searching should begin
- : If 0, replace all matches; if > 0, replace only the N-th match

## Dependencies
- Functions called/Symbols referenced:
  - check_replace_text_has_escape (analyze replacement text for optimization)
  - appendStringInfoRegexpSubstr (process replacement text with back-references)
  - RE_compile_and_cache (compile and cache regex pattern)
  - pg_regexec (execute regex search)
  - pg_mb2wchar_with_len (convert multibyte text to wide characters)
  - charlen_to_bytelen (convert character positions to byte positions)
  - appendBinaryStringInfo (append binary data to result buffer)
  - appendStringInfoText (append text without processing)
  - cstring_to_text_with_len (convert C string result to PostgreSQL text)
- Called from (representative examples):
  - textregexreplace_noopt
  - textregexreplace  
  - textregexreplace_extended

## Notes and Other Information
- This is a public function exported via varlena.h for use by regexp functions
- Handles zero-length matches correctly by advancing search position
- Automatically optimizes performance by using REG_NOSUB when no capture groups are needed
- Supports interruption via CHECK_FOR_INTERRUPTS() for long-running operations
- Located in src/backend/utils/adt/varlena.c:4206-4367
- Memory management includes proper cleanup of allocated buffers and wide character arrays