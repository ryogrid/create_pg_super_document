# p_isignore

## Location
[src/backend/tsearch/wparser_def.c:623-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L623-L628)

## Overview
A static function in PostgreSQL's text search parser that checks whether the parser is currently in ignore mode.

## Definition

```c
static int
p_isignore(TParser *prs)
```
## Detailed Description
p_isignore is a simple predicate function that returns the state of the parser's ignore flag. When the parser is in ignore mode (prs->ignore is true), this function returns 1, otherwise it returns 0. This function is used to determine whether certain tokens or character sequences should be ignored during text search parsing.

The ignore flag is typically set when the parser encounters specific patterns or contexts where subsequent tokens should not be processed as regular searchable text.

## Parameters / Member Variables
- `*prs`: Pointer to a TParser structure containing the parser state, including the ignore flag that controls whether tokens should be ignored
## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
- Called from (representative examples):
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:974)
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1061)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the wparser_def.c file
- Returns 1 when in ignore mode, 0 when not ignoring
- Simple predicate function with no side effects
- Part of PostgreSQL's full-text search token parsing functionality
- Used as a condition check in the main parsing logic to determine token handling behavior

## Simplified Source

```c
static int
p_isignore(TParser *prs)
{
    // Return 1 if parser is in ignore mode, 0 otherwise
    return (prs->ignore) ? 1 : 0;
}
```

This simplified version shows the essential logic: a simple predicate that checks the parser's ignore flag and returns the appropriate boolean value for token processing decisions.