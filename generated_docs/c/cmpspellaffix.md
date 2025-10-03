# cmpspellaffix

## Location
[src/backend/tsearch/spell.c:203-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L203-L209)

## Overview
A static comparison function used for sorting SPELL structures by their affix flag string during dictionary processing in PostgreSQL's text search functionality.

## Definition

```c
static int
cmpspellaffix(const void *s1, const void *s2)
```
## Detailed Description
The  function is a comparison function specifically designed for use with  to sort an array of SPELL structure pointers by their affix flag strings. This function is crucial in the dictionary processing phase where affix flags need to be organized alphabetically to optimize storage and lookup operations.

The function compares the  field within the  union of two SPELL structures. It uses standard string comparison () to determine the lexicographic order of the affix flags. This sorting is performed before the dictionary data is reorganized into a more efficient format where flag strings are replaced with integer indices.

## Parameters / Member Variables
- `*s1`: Pointer to the first SPELL structure pointer to compare (cast from void*)
- `*s2`: Pointer to the second SPELL structure pointer to compare (cast from void*)
## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
  - [SPELL](../S/SPELL.md) (structure type)
  - [flag](../f/flag.md) (field within SPELL structure's p union)
- Called from:
  - [NISortDictionary](../N/NISortDictionary.md) (src/backend/tsearch/spell.c:1777)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the spell.c compilation unit
- The function follows the standard qsort comparison function interface, returning:
  - Negative value if s1's flag < s2's flag  
  - Zero if s1's flag == s2's flag
  - Positive value if s1's flag > s2's flag
- Used specifically during the dictionary compilation phase before the SPELL structures are converted from using string flags to integer affix indices
- Part of PostgreSQL's Ispell dictionary implementation for full-text search functionality