# makeCompoundFlags

## Location
src/backend/tsearch/spell.c: 1622 - 1638

## Overview
Extracts compound word formation flags from an affix flag set, returning only the compound-related flags.

## Definition
```c
static uint32 makeCompoundFlags(IspellDict *Conf, int affix)
```

## Detailed Description
This function retrieves the compound word flags associated with a specific affix flag set identified by its index. It calls getCompoundAffixFlagValue() to get all flags associated with the affix, then applies FF_COMPOUNDFLAGMASK to extract only the compound-related flags. This filtering ensures that only flags relevant to compound word formation (such as COMPOUNDFLAG, COMPOUNDBEGIN, COMPOUNDMIDDLE, COMPOUNDLAST, etc.) are returned while other affix properties are masked out.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing affix data
- `affix`: Index of the affix flag set in Conf->AffixData array

## Dependencies
- Functions called/Symbols referenced:
  - Assert (parameter validation)
  - getCompoundAffixFlagValue (retrieves all flags for the affix)
  - FF_COMPOUNDFLAGMASK (mask constant for compound flags)
  - SPNode (related to spell-checking node structure)
- Called from (representative examples):
  - mkSPNode (spell-checking node creation, called twice)

## Notes and Other Information
- Returns uint32 value containing only compound-related flags
- Uses bit masking to filter out non-compound flags
- Essential for compound word processing in spell checking
- Part of the spell-checking node creation process
- Validates affix index with Assert to ensure it's within bounds
- Compound flags control how words can be combined in compound word formation