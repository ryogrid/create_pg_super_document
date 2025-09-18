# cmpaffix

## Location
src/backend/tsearch/spell.c: 311 - 348

## Overview
A static comparison function used for sorting affixes, prioritizing prefixes over suffixes and then sorting by replacement strings using appropriate comparison methods.

## Definition
```c
static int cmpaffix(const void *s1, const void *s2)
```

## Detailed Description
The `cmpaffix` function implements a specialized comparison algorithm for AFFIX structures used in spell-checking. It follows a two-level sorting hierarchy:

1. **Type-based sorting**: Prefixes (FF_PREFIX) are sorted before suffixes
2. **String-based sorting**: Within the same type, affixes are sorted by their replacement strings using different comparison methods depending on the affix type

For prefixes, it uses standard string comparison (`strcmp`) since prefix matching is done from the beginning of words. For suffixes, it uses reverse string comparison (`strbcmp`) since suffix matching is done from the end of words.

## Parameters / Member Variables
- `s1`: Pointer to the first AFFIX structure to compare
- `s2`: Pointer to the second AFFIX structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - AFFIX (structure type)
  - FF_PREFIX (constant for prefix type)
  - strcmp (standard C library function)
  - strbcmp (custom reverse string comparison function)
- Called from (representative examples):
  - NISortAffixes

## Notes and Other Information
- Returns -1 if s1 < s2, 1 if s1 > s2, and 0 if they are equal
- Used as a comparison function for qsort to organize affixes in the spell-checking module
- The function ensures prefixes are processed before suffixes in affix application
- Located in src/backend/tsearch/spell.c:311-348