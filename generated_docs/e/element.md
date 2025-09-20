# element

## Location
[src/backend/regex/regc_locale.c:376-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_locale.c#L376-L411)

## Overview
The element function maps collating-element names to character codes in PostgreSQL's regular expression engine, serving as a character name resolution mechanism for bracket expressions.

## Definition

```c
static chr
element(struct vars *v,			/* context */
		const chr *startp,		/* points to start of name */
		const chr *endp)		/* points just past end of name */
```
## Detailed Description
The element function is part of PostgreSQL's regex locale handling system, specifically designed to resolve collating-element names within bracket expressions to their corresponding character codes. The function follows a two-step resolution process:

1. **Single-character optimization**: If the name consists of only one character, it returns that character directly as its own representation.

2. **Name table lookup**: For multi-character names, it searches through a predefined table of character names (cnames) to find a matching entry and returns the corresponding character code.

The function is locale-aware and integrates with PostgreSQL's character encoding system. It uses the pg_char_and_wchar_strncmp function for proper string comparison that handles different character encodings correctly. If a name cannot be resolved, the function generates a REG_ECOLLATE error.

## Parameters / Member Variables
- : Context structure containing regex compilation state and locale information
- : Pointer to the beginning of the collating-element name to be resolved
- : Pointer to the position immediately after the last character of the name

## Dependencies
- Functions called/Symbols referenced:
  - [pg_char_and_wchar_strncmp](../p/pg_char_and_wchar_strncmp.md)
  - CHR (macro for character conversion)
  - NOTE (macro for noting regex features)
  - ERR (macro for error reporting)
  - REG_ULOCALE (regex feature flag)
  - REG_ECOLLATE (error code for collation issues)
  - cnames (global character name table)
- Called from (representative examples):
  - chrnamed
  - [brackpart](../b/brackpart.md)

## Notes and Other Information
- The function specifically avoids using hard-wired Unicode classification tables, instead relying on libc locale routines for maximum compatibility across different encodings and locales
- Part of PostgreSQL's regex engine's locale-sensitive character handling subsystem
- Returns character type 'chr' which is PostgreSQL's internal character representation
- Used primarily in regex bracket expression parsing where named character classes like [[:alpha:]] need to be resolved