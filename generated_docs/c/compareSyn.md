# compareSyn

## Location
[src/backend/tsearch/dict_synonym.c:85-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_synonym.c#L85-L91)

## Overview
A comparison function used for sorting and searching synonym entries by their input words.

## Definition

```c
static int
compareSyn(const void *a, const void *b)
```
## Detailed Description
This function implements a standard comparison function compatible with qsort() and bsearch(). It compares two Syn structures by their 'in' (input word) fields using string comparison. The function is essential for maintaining sorted synonym arrays and enabling efficient binary search operations during lexicalization.

The function follows the standard C library comparison contract:
- Returns negative value if a < b
- Returns zero if a == b  
- Returns positive value if a > b

## Parameters / Member Variables
- : Pointer to first Syn structure to compare
- : Pointer to second Syn structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (for string comparison)
  - [Syn](../S/Syn.md) (structure type being compared)
- Called from (representative examples):
  - [dsynonym_init](../d/dsynonym_init.md) (used with qsort to sort synonym array)
  - [dsynonym_lexize](../d/dsynonym_lexize.md) (used with bsearch to find matching synonyms)

## Notes and Other Information
- This is a static function used internally within the synonym dictionary module
- Essential for maintaining the sorted order required for binary search efficiency
- Follows standard C library comparison function conventions
- The comparison is based solely on the input word (in field) of the synonym entries