# matchIdSubst

## Location
[src/backend/tsearch/dict_thesaurus.c:676-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L676-L695)

## Overview
Checks if a lexeme information structure contains a variant with a specific substitution ID.

## Definition

```c
static bool
matchIdSubst(LexemeInfo *stored, uint32 idsubst)
```
## Detailed Description
The  function searches through a linked list of lexeme variants to determine if any variant has a matching substitution ID (). It iterates through the  chain of  structures, comparing each variant's  field with the target ID. The function returns true if a matching substitution ID is found, or false if no match is found. If the input  parameter is NULL, the function returns true (indicating a match with any substitution).

## Parameters / Member Variables
- `*stored`: Pointer to the first  structure in a linked list of lexeme variants to search through
- `idsubst`: The substitution ID to search for among the lexeme variants
## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for lexeme information and variant chaining)
- Called from:
  -  (at src/backend/tsearch/dict_thesaurus.c:739)

## Notes and Other Information
- Returns true if  is NULL, treating null input as a match condition
- Traverses the  linked list to check all available variants
- This is a static function, only used internally within the thesaurus dictionary module
- Used as part of the thesaurus matching algorithm to find appropriate lexeme variants for substitution