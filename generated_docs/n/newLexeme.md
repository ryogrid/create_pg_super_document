# newLexeme

## Location
[src/backend/tsearch/dict_thesaurus.c:72-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L72-L105)

## Overview
Creates and initializes a new lexeme entry in the thesaurus dictionary structure with associated substitution information.

## Definition

```c
static void
newLexeme(DictThesaurus *d, char *b, char *e, uint32 idsubst, uint16 posinsubst)
```
## Detailed Description
The newLexeme function is responsible for adding a new lexeme (word/token) to the DictThesaurus structure. It dynamically manages memory allocation for the lexeme array, automatically expanding the storage capacity when needed. The function extracts the lexeme string from a character buffer range, creates a copy in allocated memory, and initializes associated metadata including substitution ID and position information.

The function implements a dynamic array growth strategy, starting with an initial capacity of 16 entries and doubling the size when capacity is exceeded. Each lexeme is stored with its own LexemeInfo structure that tracks substitution relationships used by the thesaurus dictionary.

## Parameters / Member Variables
- `*d`: Pointer to the DictThesaurus structure where the new lexeme will be added
- `*b`: Pointer to the beginning of the lexeme string in the source buffer
- `*e`: Pointer to the end of the lexeme string in the source buffer (exclusive)
- `idsubst`: Unique identifier for the substitution rule this lexeme belongs to
- `posinsubst`: Position index of this lexeme within its substitution rule
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [repalloc](../r/repalloc.md): PostgreSQL memory reallocation function
  - memcpy: Standard C library function for memory copying
  - DictThesaurus: The main thesaurus dictionary structure
  - [TheLexeme](../T/TheLexeme.md): Structure representing individual lexeme entries
  - [LexemeInfo](../L/LexemeInfo.md): Structure containing lexeme metadata and substitution information

- Called from (representative examples):
  - [thesaurusRead](../t/thesaurusRead.md): Main parsing function that processes thesaurus configuration files

## Notes and Other Information
- This is a static function, only accessible within the dict_thesaurus.c file
- The function assumes that the input string range [b,e) is valid and properly null-terminates the copied lexeme
- Memory allocation uses PostgreSQL's palloc/repalloc functions which provide error handling and memory context management
- The dynamic array growth strategy (doubling) provides O(1) amortized insertion time
- Each lexeme maintains a linked list of LexemeInfo entries to support multiple substitution rules

## Simplified Source

```c
static void
newLexeme(DictThesaurus *d, char *b, char *e, uint32 idsubst, uint16 posinsubst)
{
    TheLexeme *ptr;

    // Expand lexeme array if needed
    if (d->nwrds >= d->ntwrds)
    {
        if (d->ntwrds == 0)
        {
            d->ntwrds = 16;
            d->wrds = (TheLexeme *) palloc(sizeof(TheLexeme) * d->ntwrds);
        }
        else
        {
            d->ntwrds *= 2;
            d->wrds = (TheLexeme *) repalloc(d->wrds, sizeof(TheLexeme) * d->ntwrds);
        }
    }

    // Initialize new lexeme entry
    ptr = d->wrds + d->nwrds;
    d->nwrds++;

    // Copy lexeme string from buffer range [b,e)
    ptr->lexeme = palloc(e - b + 1);
    memcpy(ptr->lexeme, b, e - b);
    ptr->lexeme[e - b] = '\0';

    // Initialize substitution info
    ptr->entries = (LexemeInfo *) palloc(sizeof(LexemeInfo));
    ptr->entries->nextentry = NULL;
    ptr->entries->idsubst = idsubst;
    ptr->entries->posinsubst = posinsubst;
}
```