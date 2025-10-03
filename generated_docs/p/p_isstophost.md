# p_isstophost

## Location
[src/backend/tsearch/wparser_def.c:612-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L612-L622)

## Overview
A static function in PostgreSQL's text search parser that determines whether to stop host parsing by checking and resetting the wanthost flag.

## Definition

```c
static int
p_isstophost(TParser *prs)
```
## Detailed Description
p_isstophost is a helper function used in the text search word parser to control host parsing behavior. The function checks if the parser is currently expecting a host token (indicated by the wanthost flag). If so, it resets the flag to false and returns 1 to indicate that host parsing should stop. If the parser is not expecting a host, it returns 0.

This function is part of the state machine logic that handles URL and host parsing within text search processing. It ensures that host parsing is properly terminated when the expected host token has been processed.

## Parameters / Member Variables
- `*prs`: Pointer to a TParser structure containing the parser state, including the wanthost flag that controls host parsing behavior
## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
- Called from (representative examples):
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1354)
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1368)
  - [p_isspecial](p_isspecial.md) (at src/backend/tsearch/wparser_def.c:1392)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the wparser_def.c file
- Returns 1 when host parsing should stop, 0 when it should continue
- The function has side effects - it modifies the wanthost flag when returning 1
- Part of PostgreSQL's full-text search URL/host parsing functionality
- Used in conjunction with other parsing functions to handle complex URL structures in text search