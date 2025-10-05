# cmpspell

## Location
[src/backend/tsearch/spell.c:197-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L197-L202)

## Overview
cmpspell is a comparison function used for sorting SPELL structures by their word field, compatible with qsort and other sorting functions that require a comparison callback.

## Definition
```c
static int cmpspell(const void *s1, const void *s2)
```

## Detailed Description
This function compares two SPELL structures by comparing their word fields lexicographically using strcmp(). It follows the standard C library comparison function interface, taking two const void pointers that are expected to point to SPELL pointers. The function dereferences these double pointers to access the SPELL structures and then compares their word fields. The return value follows strcmp() semantics: negative if the first word is lexicographically less than the second, positive if greater, and zero if equal.

## Parameters / Member Variables
- `s1`: Pointer to the first SPELL pointer to be compared (cast from const void *)
- `s2`: Pointer to the second SPELL pointer to be compared (cast from const void *)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
  - [SPELL](../S/SPELL.md) (struct type)
  - [word](../w/word.md) (member field of SPELL structure)
- Called from (representative examples):
  - [NISortDictionary](../N/NISortDictionary.md)

## Notes and Other Information
- This is a static function, only accessible within the spell.c compilation unit
- Used as a callback function for sorting operations (typically with qsort)
- The function assumes the parameters are pointers to SPELL pointers, not direct SPELL structures
- Essential for maintaining sorted dictionary entries for efficient lookup operations
- Follows the standard C library comparison function contract for use with sorting algorithms
- Part of the ISpell dictionary processing system in PostgreSQL's full-text search functionality

## Simplified Source

```c
static int cmpspell(const void *s1, const void *s2) {
    // Compare SPELL structures by their word field
    return strcmp((*(SPELL *const *) s1)->word,
                  (*(SPELL *const *) s2)->word);
}
```