# TheSubstitute

## Location
src/backend/tsearch/dict_thesaurus.c: 50 - 67

## Overview
TheSubstitute is a structure used in PostgreSQL's thesaurus dictionary implementation to store substitution information for lexemes, including the number of lexemes to substitute and the prepared substituted result.

## Definition
```c
typedef struct
{
	uint16		lastlexeme;		/* number lexemes to substitute */
	uint16		reslen;
	TSLexeme   *res;			/* prepared substituted result */
} TheSubstitute;
```

## Detailed Description
TheSubstitute serves as a critical data structure in the thesaurus dictionary functionality within PostgreSQL's text search system. It manages substitution operations by tracking how many lexemes need to be substituted (lastlexeme), the length of the result (reslen), and maintains a pointer to the prepared substituted result as TSLexeme structures. This structure enables efficient thesaurus-based text transformations during search operations.

## Parameters / Member Variables
- `lastlexeme`: A 16-bit unsigned integer indicating the number of lexemes that should be substituted
- `reslen`: A 16-bit unsigned integer representing the length of the substitution result
- `res`: A pointer to TSLexeme structures containing the prepared substituted result

## Dependencies
- Functions called/Symbols referenced:
  - TSDictionaryCacheEntry
  - [TheLexeme](TheLexeme.md)
  - [TheSubstitute](TheSubstitute.md) (self-reference)
- Called from (representative examples):
  - [TheSubstitute](TheSubstitute.md) (recursive structure reference)
  - [addWrd](../a/addWrd.md)
  - [copyTSLexeme](../c/copyTSLexeme.md)

## Notes and Other Information
- This structure is defined in src/backend/tsearch/dict_thesaurus.c at lines 45-50
- Works in conjunction with TheLexeme structures to provide complete thesaurus functionality
- The structure is designed to optimize substitution operations in thesaurus dictionaries
- Part of PostgreSQL's full-text search infrastructure, specifically for thesaurus-based term substitution
- Used to store precompiled substitution results for efficient lookup during search operations