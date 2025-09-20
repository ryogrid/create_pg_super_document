# p_iseqC

## Location
[src/backend/tsearch/wparser_def.c:481-486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L481-L486)

## Overview
A convenience wrapper function that checks if the current character in the parser matches the stored current character (prs->c).

## Definition

```c
static int
p_iseqC(TParser *prs)
```
## Detailed Description
The  function is a simple wrapper around the  function that compares the current character at the parser's position with the character stored in the parser's  field. This function is part of PostgreSQL's text search parser infrastructure and is used extensively in the state machine transitions for tokenizing text. It provides a convenient way to check if the current character matches the expected character without having to explicitly pass the character value.

The function internally calls , which performs the actual character comparison by checking that the current character length is 1 (ASCII) and that the byte at the current position matches the specified character.

## Parameters / Member Variables
- : Pointer to a TParser structure containing the current parsing state, including the current character ( field) and position information

## Dependencies
- Functions called/Symbols referenced:
  - p_iseq (performs the actual character comparison)
  - [TParser](../T/TParser.md) (structure type)
- Called from (representative examples):
  - [p_isspecial](p_isspecial.md) (used extensively throughout the state machine for character-specific transitions)
  - [_make_compiler_happy](../m/_make_compiler_happy.md) (test/debug function)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Returns 1 if the current character matches prs->c, 0 otherwise
- Used extensively in the state machine definition tables for text parsing
- The underlying p_iseq function should only be used for ASCII symbols according to code comments
- Part of the character classification system for the text search word parser
- Critical for state transitions in tokenization, especially for detecting specific delimiter characters