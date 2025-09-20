# cmpcmdflag

## Location
[src/backend/tsearch/spell.c:210-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L210-L228)

## Overview
A static comparison function used for sorting and binary searching CompoundAffixFlag structures in PostgreSQL's Hunspell dictionary processing, handling different flag representation modes.

## Definition

```c
static int
cmpcmdflag(const void *f1, const void *f2)
```
## Detailed Description
The  function is a comparison function designed for use with  and  to manage arrays of CompoundAffixFlag structures. This function handles the complexity of comparing flags that can be represented in different formats depending on the flag mode of the Hunspell dictionary being processed.

The function supports three different flag representation modes:
- FM_CHAR: Single character flags (like traditional ispell)
- FM_LONG: Two character flags  
- FM_NUM: Numeric flags (0 to 65535)

For numeric flags (FM_NUM), it performs integer comparison, while for character-based flags (FM_CHAR and FM_LONG), it uses string comparison. The function ensures that both flags being compared use the same flag mode via an assertion.

## Parameters / Member Variables
- : Pointer to the first CompoundAffixFlag structure to compare (cast from void*)
- : Pointer to the second CompoundAffixFlag structure to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function for string flags)
  - Assert (macro for validating matching flag modes)
  - CompoundAffixFlag (structure type)
  - FlagMode (enum type)
  - FM_NUM (flag mode constant for numeric flags)
- Called from:
  - [getCompoundAffixFlagValue](../g/getCompoundAffixFlagValue.md) (src/backend/tsearch/spell.c:1145) - for binary search
  - NIImportOOAffixes (src/backend/tsearch/spell.c:1292) - for sorting during dictionary import

## Notes and Other Information
- This is a static function, accessible only within the spell.c compilation unit
- Follows the standard comparison function interface for qsort/bsearch:
  - Returns negative value if f1 < f2
  - Returns 0 if f1 == f2  
  - Returns positive value if f1 > f2
- The assertion ensures both structures use the same flag representation mode before comparison
- Used specifically in Hunspell dictionary processing where compound word affixes need to be efficiently searched and organized
- Part of PostgreSQL's advanced text search functionality supporting Hunspell dictionary format