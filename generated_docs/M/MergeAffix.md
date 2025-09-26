# MergeAffix

## Location
[src/backend/tsearch/spell.c:1575-1621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1575-L1621)

## Overview
Merges two affix flag sets and stores the result as a new entry in the dictionary's affix data array.

## Definition
```c
static int MergeAffix(IspellDict *Conf, int a1, int a2)
```

## Detailed Description
This function combines two existing affix flag sets identified by their indices into a single merged flag set. It handles different flag modes appropriately: for numeric flag mode (FM_NUM), flags are separated by commas, while for character-based modes, flags are concatenated directly. The function automatically expands the AffixData array if necessary and returns the index of the newly created merged flag set. Empty flag sets are handled as special cases where the non-empty set is returned without modification.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing affix data
- `a1`: Index of the first affix flag set to merge
- `a2`: Index of the second affix flag set to merge

## Dependencies
- Functions called/Symbols referenced:
  - Assert (parameter validation)
  - [repalloc](../r/repalloc.md) (memory reallocation for AffixData expansion)
  - cpalloc (memory allocation for new flag string)
  - sprintf (string formatting for flag concatenation)
  - FM_NUM (flag mode constant for numeric flags)
- Called from (representative examples):
  - [mkSPNode](../m/mkSPNode.md) (spell-checking node creation)

## Notes and Other Information
- Returns index of the merged affix flag set in Conf->AffixData
- Automatically doubles AffixData array size when space is needed
- Handles empty flag sets by returning the non-empty set's index
- For FM_NUM mode, uses comma separation between flag values
- For character modes (FM_CHAR, FM_LONG), concatenates flags directly
- Memory allocation uses cpalloc for proper memory context management
- Function maintains NULL termination of the AffixData array