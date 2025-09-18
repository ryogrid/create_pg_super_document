# SpecialFURL

## Location
[src/backend/tsearch/wparser_def.c:588-595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L588-L595)

## Overview
A static function that configures the parser to recognize URL/hostname patterns by setting the wanthost flag and rewinding the parser position.

## Definition
```c
static void SpecialFURL(TParser *prs)
```

## Detailed Description
This function is called when the parser encounters a pattern that suggests the beginning of a URL or hostname (such as "ftp://", "http://", etc.). It performs two key operations:

1. **Sets the wanthost flag**: Indicates to the parser that it should expect and recognize hostname patterns in the upcoming text
2. **Rewinds parser position**: Moves the parser position backward by the length of the current token, effectively "unprocessing" the current token so it can be re-parsed in the context of URL/hostname recognition

This mechanism allows the parser to dynamically switch parsing modes when URL patterns are detected, ensuring proper tokenization of web addresses and hostnames.

## Parameters / Member Variables
- `prs`: Pointer to TParser structure containing the current parsing state, position information, and control flags

## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
- Called from (representative examples):
  - [p_isspecial](../p/p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1478)

## Notes and Other Information
- The function name suggests it handles "File URL" patterns, likely including protocols like ftp://, http://, https://, etc.
- The rewind mechanism (subtracting lenbytetoken and lenchartoken) ensures that URL patterns are processed as complete units
- Part of PostgreSQL's text search parser that needs to recognize different types of content including URLs
- The wanthost flag likely triggers specialized hostname parsing logic elsewhere in the parser
- This function enables context-sensitive parsing where the same text might be tokenized differently depending on whether it appears in a URL context