# p_isEOF

## Location
[src/backend/tsearch/wparser_def.c:474-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L474-L480)

## Overview
A static utility function that checks whether the text search parser has reached the end of the input string during parsing operations.

## Definition

```c
static int
p_isEOF(TParser *prs)
```
## Detailed Description
The  function is a helper utility in PostgreSQL's text search parser that determines if the parser has reached the end of the input string. It performs this check by examining two conditions: whether the current byte position has reached the end of the string length, or whether the character length is zero. The function is designed to be used internally within the text search parsing framework to control parsing loops and determine when to stop processing input text.

This function is part of the word parser definition module () which handles the tokenization and parsing of text for full-text search operations in PostgreSQL.

## Parameters / Member Variables
- `*prs`: Pointer to a TParser structure that contains the current parsing state, including position information and string length
## Dependencies
- Functions called/Symbols referenced:
  - [TParser](../T/TParser.md) (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - [p_isspecial](p_isspecial.md) (extensively used throughout the function)
  - [_make_compiler_happy](../m/_make_compiler_happy.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- The function uses an Assert to ensure the parser state is valid before checking EOF conditions
- Returns 1 if at EOF, 0 otherwise
- Critical for controlling parsing loops in the text search tokenization process
- Used extensively by the p_isspecial function which handles special character detection during parsing