# NIImportOOAffixes

## Location
[src/backend/tsearch/spell.c:1199-1427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1199-L1427)

## Overview
Imports affix files that follow MySpell or Hunspell format, parsing compound flags and affix rules to configure an Ispell dictionary.

## Definition
```c
static void NIImportOOAffixes(IspellDict *Conf, const char *filename)
```

## Detailed Description
This function reads and parses MySpell/Hunspell format affix files (.aff files) in two passes. The first pass identifies compound flags and flag modes (COMPOUNDFLAG, COMPOUNDBEGIN, etc.), while the second pass processes prefix (PFX) and suffix (SFX) affix rules. It supports flag alias compression (AF parameter) and handles various compound word formation flags. The function configures the dictionary for compound word processing and sets up affix transformation rules based on the parsed data.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure to be configured with parsed affix data
- `filename`: Path to the affix file to be imported (.aff file)

## Dependencies
- Functions called/Symbols referenced:
  - tsearch_readline_begin/tsearch_readline/tsearch_readline_end (file reading utilities)
  - addCompoundAffixFlagValue (processes compound flags)
  - parse_ooaffentry (parses individual affix entries)
  - lowerstr_ctx (string case conversion)
  - getCompoundAffixFlagValue/getAffixFlagSet (flag processing)
  - NIAddAffix (adds parsed affix to dictionary)
  - qsort/cmpcmdflag (sorts compound flags)
- Called from (representative examples):
  - NIImportAffixes (main affix import function)

## Notes and Other Information
- Supports three flag modes: FM_CHAR (single character), FM_LONG (two characters), FM_NUM (numeric)
- Handles compound word flags: COMPOUNDFLAG, COMPOUNDBEGIN, COMPOUNDLAST, COMPOUNDMIDDLE, ONLYINCOMPOUND, COMPOUNDPERMITFLAG, COMPOUNDFORBIDFLAG
- Implements alias compression feature (AF parameter) to reduce memory usage
- Processes both prefix (PFX) and suffix (SFX) transformation rules
- Cross-product flag (FF_CROSSPRODUCT) allows combining prefixes and suffixes
- Error handling for invalid flag configurations and file access issues