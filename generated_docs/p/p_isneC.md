# p_isneC

## Location
[src/backend/tsearch/wparser_def.c:487-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L487-L492)

## Overview
A convenience wrapper function that checks if the current character in the parser does NOT match the stored current character (prs->c).

## Definition

```c
static int
p_isneC(TParser *prs)
```
## Detailed Description
The  function is the logical complement of , providing a convenient way to check if the current character at the parser's position does NOT match the character stored in the parser's  field. This function is part of PostgreSQL's text search parser infrastructure and returns the negated result of .

While  checks for equality,  checks for inequality, which is useful in state machine logic where you need to determine when the current character differs from an expected character. This function provides a more readable alternative to manually negating the result of .

## Parameters / Member Variables
- `*prs`: Pointer to a TParser structure containing the current parsing state, including the current character ( field) and position information
## Dependencies
- Functions called/Symbols referenced:
  - p_iseq (performs the actual character comparison, result is negated)
  - [TParser](../T/TParser.md) (structure type)
- Called from (representative examples):
  - [_make_compiler_happy](../m/_make_compiler_happy.md) (test/debug function)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Returns 1 if the current character does NOT match prs->c, 0 if it does match
- Logically equivalent to 
- Less frequently used compared to  in the codebase
- Part of the character classification system for the text search word parser
- Provides semantic clarity when checking for character inequality in parsing logic