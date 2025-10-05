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

## Simplified Source

```c
static int MergeAffix(IspellDict *Conf, int a1, int a2) {
    char **ptr;

    Assert(a1 < Conf->nAffixData && a2 < Conf->nAffixData);

    // Return non-empty set if one is empty
    if (*Conf->AffixData[a1] == '\0')
        return a2;
    else if (*Conf->AffixData[a2] == '\0')
        return a1;

    // Expand array if needed
    if (Conf->nAffixData + 1 >= Conf->lenAffixData) {
        Conf->lenAffixData *= 2;
        Conf->AffixData = (char **) repalloc(Conf->AffixData,
                                           sizeof(char *) * Conf->lenAffixData);
    }

    // Create new merged flag string
    ptr = Conf->AffixData + Conf->nAffixData;
    if (Conf->flagMode == FM_NUM) {
        // Numeric mode: separate with comma
        *ptr = cpalloc(strlen(Conf->AffixData[a1]) +
                      strlen(Conf->AffixData[a2]) + 2); // +1 for comma, +1 for \0
        sprintf(*ptr, "%s,%s", Conf->AffixData[a1], Conf->AffixData[a2]);
    } else {
        // Character mode: concatenate directly
        *ptr = cpalloc(strlen(Conf->AffixData[a1]) +
                      strlen(Conf->AffixData[a2]) + 1); // +1 for \0
        sprintf(*ptr, "%s%s", Conf->AffixData[a1], Conf->AffixData[a2]);
    }

    // Maintain NULL termination and update count
    ptr++;
    *ptr = NULL;
    Conf->nAffixData++;

    return Conf->nAffixData - 1;
}
```