# getCompoundAffixFlagValue

## Location
[src/backend/tsearch/spell.c:1125-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1125-L1160)

## Overview
Retrieves the combined bit flags corresponding to a set of compound affix flags specified in a string, using binary search for efficient lookup.

## Definition
```c
static int getCompoundAffixFlagValue(IspellDict *Conf, char *s)
```

## Detailed Description
This function parses a string containing multiple affix flags and returns a combined integer value representing all the compound affix parameters. It processes the flags iteratively:

1. Uses getNextFlagFromString to extract individual flags from the input string
2. For each flag, creates a temporary CompoundAffixFlag key using setCompoundAffixFlagValue
3. Performs a binary search using bsearch to find the flag in the sorted CompoundAffixFlags array
4. If found, combines the flag's value with the result using bitwise OR operations
5. Returns the accumulated flag value, or 0 if no compound flags are configured

The function efficiently handles multiple flag formats and returns 0 immediately if no compound affix flags are defined in the dictionary.

## Parameters / Member Variables
- `Conf`: Pointer to the IspellDict containing the compound affix flag configuration
- `s`: String containing one or more affix flags to look up

## Dependencies
- Functions called/Symbols referenced:
  - [getNextFlagFromString](getNextFlagFromString.md): Extract individual flags from the input string
  - [setCompoundAffixFlagValue](../s/setCompoundAffixFlagValue.md): Create a search key with the parsed flag
  - bsearch: Standard C library binary search function
  - [cmpcmdflag](../c/cmpcmdflag.md): Comparison function for CompoundAffixFlag structures
- Called from (representative examples):
  - [NIImportOOAffixes](../N/NIImportOOAffixes.md): Process OpenOffice-style affix configurations
  - [makeCompoundFlags](../m/makeCompoundFlags.md): Generate compound flags for word formation

## Notes and Other Information
- Returns 0 if no compound affix flags are configured (nCompoundAffixFlag == 0)
- Uses bitwise OR to combine multiple flag values into a single result
- Requires the CompoundAffixFlags array to be sorted for binary search to work correctly
- Part of PostgreSQL's Hunspell-compatible spell checking system
- Efficiently handles multiple flag representations through the flag parsing infrastructure
- The returned integer can be used as a bitmask for compound word processing rules

## Simplified Source

```c
static int getCompoundAffixFlagValue(IspellDict *Conf, char *s) {
    uint32 flag = 0;
    CompoundAffixFlag *found, key;
    char sflag[BUFSIZ];
    char *flagcur;

    // Return 0 if no compound flags configured
    if (Conf->nCompoundAffixFlag == 0)
        return 0;

    // Process each flag in the input string
    flagcur = s;
    while (*flagcur) {
        // Extract next flag from string
        getNextFlagFromString(Conf, &flagcur, sflag);

        // Create search key for binary search
        setCompoundAffixFlagValue(Conf, &key, sflag, 0);

        // Find flag in sorted array
        found = (CompoundAffixFlag *)
            bsearch(&key, Conf->CompoundAffixFlags,
                   Conf->nCompoundAffixFlag, sizeof(CompoundAffixFlag),
                   cmpcmdflag);

        // Combine flag value if found
        if (found != NULL)
            flag |= found->value;
    }

    return flag;
}
```