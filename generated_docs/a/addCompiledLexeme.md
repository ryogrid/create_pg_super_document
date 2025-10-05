# addCompiledLexeme

## Location
[src/backend/tsearch/dict_thesaurus.c:303-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L303-L333)

## Overview
Adds a compiled lexeme entry to the processed thesaurus word array, expanding storage capacity as needed during dictionary compilation.

## Definition
```c
static TheLexeme *addCompiledLexeme(TheLexeme *newwrds, int *nnw, int *tnm, TSLexeme *lexeme, LexemeInfo *src, uint16 tnvariant)
```

## Detailed Description
The addCompiledLexeme function is used during thesaurus dictionary compilation to create processed lexeme entries from parsed TSLexeme structures. It manages dynamic memory allocation for the compiled lexeme array, doubling the capacity when more space is needed. The function creates a new TheLexeme entry with associated LexemeInfo metadata, handling both normal lexemes and placeholder entries (when lexeme is NULL).

This function is part of the compilation phase that transforms the raw parsed thesaurus data into the optimized runtime format used for text search operations. Each compiled lexeme maintains information about its source substitution rule and position for efficient pattern matching.

## Parameters / Member Variables
- `newwrds`: Pointer to the array of compiled TheLexeme structures being built
- `nnw`: Pointer to the current number of entries in the newwrds array (modified by reference)
- `tnm`: Pointer to the total allocated capacity of the newwrds array (modified by reference)
- `lexeme`: Source TSLexeme structure containing the word text and flags (can be NULL)
- `src`: Source LexemeInfo structure containing substitution rule metadata
- `tnvariant`: Total number of variants for this lexeme entry

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md): PostgreSQL memory reallocation function for expanding arrays
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function for new LexemeInfo entries
  - [pstrdup](../p/pstrdup.md): PostgreSQL string duplication function
  - [TheLexeme](../T/TheLexeme.md): Structure representing compiled lexeme entries
  - [LexemeInfo](../L/LexemeInfo.md): Structure containing lexeme metadata and substitution information
  - TSLexeme: Structure representing parsed lexeme data

- Called from (representative examples):
  - [compileTheLexeme](../c/compileTheLexeme.md): Dictionary compilation function that processes parsed thesaurus data

## Notes and Other Information
- This is a static function, only accessible within the dict_thesaurus.c file
- Returns the potentially reallocated newwrds array pointer (address may change due to repalloc)
- Handles NULL lexeme entries by setting lexeme to NULL and tnvariant to 1
- Uses a doubling strategy for array growth to achieve amortized O(1) insertion time
- Each compiled lexeme gets a single LexemeInfo entry (no linked list like the parsing phase)
- The function preserves substitution rule metadata (idsubst and posinsubst) from the source
- Memory management relies on PostgreSQL's palloc system with automatic cleanup

## Simplified Source

```c
static TheLexeme *
addCompiledLexeme(TheLexeme *newwrds, int *nnw, int *tnm, TSLexeme *lexeme, LexemeInfo *src, uint16 tnvariant)
{
    // Expand array if needed (doubling strategy)
    if (*nnw >= *tnm)
    {
        *tnm *= 2;
        newwrds = (TheLexeme *) repalloc(newwrds, sizeof(TheLexeme) * *tnm);
    }

    // Initialize new lexeme entry
    newwrds[*nnw].entries = (LexemeInfo *) palloc(sizeof(LexemeInfo));

    // Copy lexeme data or set as NULL placeholder
    if (lexeme && lexeme->lexeme)
    {
        newwrds[*nnw].lexeme = pstrdup(lexeme->lexeme);
        newwrds[*nnw].entries->tnvariant = tnvariant;
    }
    else
    {
        newwrds[*nnw].lexeme = NULL;  // Placeholder (stop word marker)
        newwrds[*nnw].entries->tnvariant = 1;
    }

    // Copy substitution metadata from source
    newwrds[*nnw].entries->idsubst = src->idsubst;
    newwrds[*nnw].entries->posinsubst = src->posinsubst;
    newwrds[*nnw].entries->nextentry = NULL;

    (*nnw)++;
    return newwrds;  // May be reallocated address
}
```