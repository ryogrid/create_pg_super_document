# NISortDictionary

## Location
[src/backend/tsearch/spell.c:1721-1829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1721-L1829)

## Overview
NISortDictionary builds the dictionary tree and optimizes affix data from the imported dictionary and affixes for efficient text search operations.

## Definition

```c
void
NISortDictionary(IspellDict *Conf)
```
## Detailed Description
This function performs critical post-import processing of dictionary data to build an efficient prefix tree structure for spell checking. It handles two different modes based on whether flag aliases are being used:

1. **Flag Aliases Mode**: When  is enabled, it validates and converts string affix flags to integer indices referencing pre-built AffixData.

2. **Standard Mode**: When flag aliases are not used, it:
   - Counts unique flags in the dictionary
   - Sorts spell entries by affix flags using 
   - Creates the AffixData array with unique flag strings
   - Replaces textual flags with integer indices for efficient lookups

After processing affixes, it sorts all spell entries lexicographically and builds a prefix tree (SPNode structure) for fast dictionary lookups.

## Parameters / Member Variables
- `*Conf`: Pointer to IspellDict structure containing the dictionary configuration and data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - qsort
  - [cmpspellaffix](../c/cmpspellaffix.md)  
  - [cmpspell](../c/cmpspell.md)
  - [mkSPNode](../m/mkSPNode.md)
  - [cpstrdup](../c/cpstrdup.md)
  - [t_isdigit](../t/t_isdigit.md)
  - [t_isspace](../t/t_isspace.md)
  - strtol
  - strlen
- Called from (representative examples):
  - [dispell_init](../d/dispell_init.md)

## Notes and Other Information
- This function must be called after NIImportAffixes and NIImportDictionary but before dictionary can be used for spell checking
- Validates affix alias ranges and format, throwing CONFIG_FILE_ERROR for invalid aliases
- The resulting Dictionary tree enables efficient prefix-based lookups during spell checking operations
- Memory allocation uses palloc0 for the AffixData array
- Handles empty flag cases by assigning index 0 in the AffixData array