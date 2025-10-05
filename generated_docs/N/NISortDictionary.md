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

## Simplified Source

```c
void NISortDictionary(IspellDict *Conf) {
    int i, naffix, curaffix;

    // Process affix data based on mode
    if (Conf->useFlagAliases) {
        // Flag aliases mode: convert string flags to indices
        for (i = 0; i < Conf->nspell; i++) {
            char *end;

            if (*Conf->Spell[i]->p.flag != '\0') {
                // Parse flag as integer index
                errno = 0;
                curaffix = strtol(Conf->Spell[i]->p.flag, &end, 10);

                // Validate conversion
                if (Conf->Spell[i]->p.flag == end || errno == ERANGE)
                    ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                                  errmsg("invalid affix alias \"%s\"",
                                        Conf->Spell[i]->p.flag)));

                // Validate range
                if (curaffix < 0 || curaffix >= Conf->nAffixData)
                    ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                                  errmsg("invalid affix alias \"%s\"",
                                        Conf->Spell[i]->p.flag)));

                // Validate format
                if (*end != '\0' && !t_isdigit(end) && !t_isspace(end))
                    ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                                  errmsg("invalid affix alias \"%s\"",
                                        Conf->Spell[i]->p.flag)));
            } else {
                // Empty flag gets index 0
                curaffix = 0;
            }

            Conf->Spell[i]->p.d.affix = curaffix;
            Conf->Spell[i]->p.d.len = strlen(Conf->Spell[i]->word);
        }
    } else {
        // Standard mode: build AffixData from unique flags

        // Sort by affix flags to group identical flags
        qsort(Conf->Spell, Conf->nspell, sizeof(SPELL *), cmpspellaffix);

        // Count unique flags
        naffix = 0;
        for (i = 0; i < Conf->nspell; i++) {
            if (i == 0 || strcmp(Conf->Spell[i]->p.flag, Conf->Spell[i - 1]->p.flag) != 0)
                naffix++;
        }

        // Allocate AffixData array
        Conf->AffixData = (char **) palloc0(naffix * sizeof(char *));

        // Fill AffixData and convert flags to indices
        curaffix = -1;
        for (i = 0; i < Conf->nspell; i++) {
            if (i == 0 || strcmp(Conf->Spell[i]->p.flag, Conf->AffixData[curaffix]) != 0) {
                curaffix++;
                Assert(curaffix < naffix);
                Conf->AffixData[curaffix] = cpstrdup(Conf, Conf->Spell[i]->p.flag);
            }

            Conf->Spell[i]->p.d.affix = curaffix;
            Conf->Spell[i]->p.d.len = strlen(Conf->Spell[i]->word);
        }

        Conf->lenAffixData = Conf->nAffixData = naffix;
    }

    // Build prefix tree from sorted dictionary
    qsort(Conf->Spell, Conf->nspell, sizeof(SPELL *), cmpspell);
    Conf->Dictionary = mkSPNode(Conf, 0, Conf->nspell, 0);
}
```