# p_isurlchar

## Location
[src/backend/tsearch/wparser_def.c:505-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L505-L536)

## Overview
A static function that determines whether the current character in a text parser is a valid URL character according to RFC 3986 standards.

## Definition

```c
static int
p_isurlchar(TParser *prs)
```
## Detailed Description
This function validates whether the character at the current position in the text parser is suitable for inclusion in a URL. It performs multiple checks:

1. **Character length validation**: Only single-byte (ASCII) characters are accepted
2. **Control character filtering**: Rejects characters <= 0x20 (space and control characters) and >= 0x7F (extended ASCII)
3. **RFC 3986 compliance**: Explicitly rejects characters that are disallowed by RFC 3986 for URLs

The function is designed as part of PostgreSQL's text search parser to identify valid URL components during text parsing operations.

## Parameters / Member Variables
- `*prs`: Pointer to TParser structure containing the current parsing state, character position, and text being parsed
## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
  - [_make_compiler_happy](../m/_make_compiler_happy.md) (at line 535)
- Called from (representative examples):
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1462)
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1472)

## Notes and Other Information
- Returns 1 if the character is a valid URL character, 0 otherwise
- The function strictly adheres to RFC 3986 URL character restrictions
- Specifically rejects: double quote, less-than, greater-than, backslash, caret, backtick, curly braces, and pipe characters
- Used within PostgreSQL's text search functionality to properly tokenize URLs in text content
- The function assumes the parser is positioned at a valid character location

## Simplified Source

```c
static int p_isurlchar(TParser *prs) {
    char ch;

    // Only accept single-byte characters
    if (prs->state->charlen != 1)
        return 0;

    ch = *(prs->str + prs->state->posbyte);

    // Reject control characters and extended ASCII
    if (ch <= 0x20 || ch >= 0x7F)
        return 0;

    // Reject RFC 3986 disallowed characters
    switch (ch) {
        case '"': case '<': case '>': case '\\':
        case '^': case '`': case '{': case '|': case '}':
            return 0;
    }

    return 1;
}
```