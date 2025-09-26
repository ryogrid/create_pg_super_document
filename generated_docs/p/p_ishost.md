# p_ishost

## Location
[src/backend/tsearch/wparser_def.c:629-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L629-L656)

## Overview
A static function in PostgreSQL's text search parser that attempts to parse a host token by creating a temporary parser copy and checking if the next token is of type HOST.

## Definition

```c
static int
p_ishost(TParser *prs)
```
## Detailed Description
p_ishost is a lookahead function that determines whether the current parsing position contains a valid host token. It works by creating a temporary copy of the parser state, setting the wanthost flag to true, and then attempting to parse the next token. If the parsed token is of type HOST, the function updates the original parser's position and length counters to include the host token and returns 1. Otherwise, it returns 0.

This function is essential for URL parsing within text search, allowing the parser to identify and properly handle host components of URLs. The function uses a temporary parser copy to avoid modifying the original parser state unless a valid host is found.

The function includes stack depth checking before recursing to prevent stack overflow in complex parsing scenarios.

## Parameters / Member Variables
- : Pointer to a TParser structure containing the current parser state

## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
  - [TParserCopyInit](../T/TParserCopyInit.md) (creates a copy of the parser)
  - [check_stack_depth](../c/check_stack_depth.md) (checks for stack overflow)
  - [TParserGet](../T/TParserGet.md) (parses the next token)
  - HOST (token type constant)
  - [TParserCopyClose](../T/TParserCopyClose.md) (cleans up the temporary parser)
- Called from (representative examples):
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1393)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the wparser_def.c file
- Returns 1 if a valid host token is found, 0 otherwise
- Uses a temporary parser copy to avoid side effects on the original parser unless successful
- Updates multiple parser state fields when a host is successfully parsed (position, length, character length)
- Part of PostgreSQL's full-text search URL parsing functionality
- Includes recursive call protection through stack depth checking
- The function has side effects only when successful - it modifies the original parser's position and length counters