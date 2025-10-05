# isAffixInUse

## Location
[src/backend/tsearch/spell.c:1961-1975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1961-L1975)

## Overview
isAffixInUse checks whether a specific affix flag is actually used by dictionary words, ensuring only referenced affixes are processed.

## Definition

```c
static bool
isAffixInUse(IspellDict *Conf, char *affixflag)
```
## Detailed Description
This function verifies if a given affix flag is actively used by dictionary entries. It searches through the AffixData array, which contains the consolidated list of affix flags that are actually referenced by words in the dictionary file.

The function iterates through all entries in the AffixData array and uses IsAffixFlagInUse() to check if the specified flag exists in any of the affix flag sets. This validation is important because:

1. **Efficiency**: Only processes affixes that are actually used
2. **Correctness**: Prevents processing of undefined or unused affix patterns  
3. **Optimization**: Reduces memory usage and processing time by filtering out unreferenced affixes

The AffixData array is populated during dictionary import and contains only the affix flags that appear in actual dictionary entries, making this a reliable check for affix usage.

## Parameters / Member Variables
- `*Conf`: Pointer to IspellDict containing the dictionary configuration and affix data
- `*affixflag`: String representing the affix flag to check for usage
## Dependencies
- Functions called/Symbols referenced:
  - [IsAffixFlagInUse](../I/IsAffixFlagInUse.md)
  - IspellDict
- Called from (representative examples):
  - [NISortAffixes](../N/NISortAffixes.md)

## Notes and Other Information
- Returns true if the affix flag is found in any AffixData entry, false otherwise
- The search is performed across all nAffixData entries in the AffixData array
- This function is typically called during affix tree construction to filter out unused affix rules
- The underlying IsAffixFlagInUse function handles the actual string parsing and comparison
- Empty affix flags are handled specially by the underlying IsAffixFlagInUse function
- Used as a validation step to ensure only dictionary-referenced affixes are included in processing

## Simplified Source

```c
static bool
isAffixInUse(IspellDict *Conf, char *affixflag)
{
    // Check if affix flag exists in any AffixData entry
    for (int i = 0; i < Conf->nAffixData; i++) {
        if (IsAffixFlagInUse(Conf, i, affixflag)) {
            return true;
        }
    }

    return false;
}
```