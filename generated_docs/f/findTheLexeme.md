# findTheLexeme

## Location
[src/backend/tsearch/dict_thesaurus.c:657-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L657-L675)

## Overview
Searches for a lexeme in the thesaurus dictionary's compiled word list and returns associated lexeme information if found.

## Definition

```c
static LexemeInfo *
findTheLexeme(DictThesaurus *d, char *lexeme)
```
## Detailed Description
The  function performs a binary search lookup on the thesaurus dictionary's sorted array of lexemes (). It searches for a specific lexeme string and returns a pointer to the corresponding  structure if the lexeme is found in the thesaurus. The function uses a temporary  structure as a search key and employs the  comparison function for the binary search operation.

## Parameters / Member Variables
- `*d`: Pointer to the  structure containing the compiled thesaurus data
- `*lexeme`: The lexeme string to search for in the thesaurus dictionary
## Dependencies
- Functions called/Symbols referenced:
  -  (structure type)
  -  (structure type for search key and array elements)
  -  (comparison function for binary search)
  -  (standard C library binary search function)
- Called from:
  -  (at src/backend/tsearch/dict_thesaurus.c:838)
  -  (at src/backend/tsearch/dict_thesaurus.c:853)

## Notes and Other Information
- Returns NULL if the dictionary has no words () or if the lexeme is not found
- The function assumes that  array is properly sorted for binary search to work correctly
- This is a static function, only used internally within the thesaurus dictionary module
- The returned  pointer provides access to the lexeme's substitution patterns and variants

## Simplified Source

```c
static LexemeInfo *findTheLexeme(DictThesaurus *d, char *lexeme) {
    // Check if dictionary has words
    if (d->nwrds == 0)
        return NULL;

    // Create search key with target lexeme
    TheLexeme key;
    key.lexeme = lexeme;
    key.entries = NULL;

    // Binary search in sorted lexeme array
    TheLexeme *res = bsearch(&key, d->wrds, d->nwrds,
                            sizeof(TheLexeme), cmpLexemeQ);

    // Return lexeme info if found, NULL otherwise
    if (res == NULL)
        return NULL;
    return res->entries;
}
```