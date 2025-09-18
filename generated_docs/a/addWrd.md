# addWrd

## Location
src/backend/tsearch/dict_thesaurus.c: 106 - 161

## Overview
Adds a replacement word to the thesaurus substitution rule, managing dynamic storage for both substitution rules and their result lexemes.

## Definition
```c
static void addWrd(DictThesaurus *d, char *b, char *e, uint32 idsubst, uint16 nwrd, uint16 posinsubst, bool useasis)
```

## Detailed Description
The addWrd function manages the storage of replacement words in thesaurus substitution rules. It handles two levels of dynamic memory management: the substitution rule array and the result lexeme arrays within each rule. When adding the first word of a new substitution (nwrd == 0), it initializes or expands the substitution array as needed. For each word, it creates a TSLexeme entry in the results array of the specified substitution rule.

The function uses static variables to track the current result array size, allowing efficient sequential addition of multiple replacement words to the same substitution rule. Each replacement word can be marked with flags such as DT_USEASIS to control how text search processing should handle the lexeme.

## Parameters / Member Variables
- `d`: Pointer to the DictThesaurus structure being populated
- `b`: Pointer to the beginning of the replacement word string
- `e`: Pointer to the end of the replacement word string (exclusive)
- `idsubst`: Unique identifier of the substitution rule this word belongs to
- `nwrd`: Sequence number of this word within the substitution rule (0-based)
- `posinsubst`: Total number of lexemes in the substitution pattern
- `useasis`: Flag indicating whether to preserve the word as-is during text search processing

## Dependencies
- Functions called/Symbols referenced:
  - palloc: PostgreSQL memory allocation function
  - repalloc: PostgreSQL memory reallocation function
  - memcpy: Standard C library function for memory copying
  - DictThesaurus: Main thesaurus dictionary structure
  - TheSubstitute: Structure representing a complete substitution rule
  - TSLexeme: Structure representing individual result lexemes
  - DT_USEASIS: Flag constant for lexeme processing behavior

- Called from (representative examples):
  - thesaurusRead: Parser function that processes thesaurus configuration files

## Notes and Other Information
- This is a static function, only accessible within the dict_thesaurus.c file
- Uses static variables nres and ntres to maintain state across multiple calls for the same substitution rule
- The function implements a null-terminated array pattern for TSLexeme results
- Dynamic array growth follows a doubling strategy starting from initial capacities (16 for substitutions, 2 for results)
- The DT_USEASIS flag prevents morphological processing of the replacement word during text search
- Memory management relies on PostgreSQL's palloc system with automatic cleanup on transaction end