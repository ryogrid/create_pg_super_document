# cmpLexemeInfo

## Location
[src/backend/tsearch/dict_thesaurus.c:334-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L334-L355)

## Overview
Compares two LexemeInfo structures for ordering purposes during thesaurus dictionary compilation and sorting operations.

## Definition
```c
static int cmpLexemeInfo(LexemeInfo *a, LexemeInfo *b)
```

## Detailed Description
The cmpLexemeInfo function implements a three-level hierarchical comparison between LexemeInfo structures. It establishes a total ordering based on substitution rule ID (idsubst), position within the substitution (posinsubst), and variant count (tnvariant). This comparison function is used during dictionary compilation to sort and organize lexeme entries for efficient runtime lookup operations.

The function follows the standard C comparison convention, returning negative, zero, or positive values to indicate less-than, equal, or greater-than relationships respectively. The hierarchical ordering ensures that lexemes from the same substitution rule are grouped together, with positions ordered sequentially and variants ordered by count.

## Parameters / Member Variables
- `a`: Pointer to the first LexemeInfo structure to compare
- `b`: Pointer to the second LexemeInfo structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [LexemeInfo](../L/LexemeInfo.md): Structure containing lexeme metadata including substitution rule information

- Called from (representative examples):
  - [cmpTheLexeme](cmpTheLexeme.md): Lexeme comparison function used in sorting operations
  - [compileTheLexeme](compileTheLexeme.md): Dictionary compilation function that needs to organize lexeme data

## Notes and Other Information
- This is a static function, only accessible within the dict_thesaurus.c file
- Returns standard C comparison values: -1 (less), 0 (equal), 1 (greater)
- Handles NULL pointer inputs by returning 0 (treating them as equal)
- Implements a hierarchical comparison order: idsubst → posinsubst → tnvariant
- Used as a comparison function for sorting algorithms during dictionary compilation
- Ensures consistent ordering of lexemes for predictable and efficient dictionary operations
- The comparison hierarchy supports grouping related lexemes together in the compiled dictionary structure