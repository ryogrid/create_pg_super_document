# CompoundAffixFlag

## Location
[src/include/tsearch/dicts/spell.h:168-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/dicts/spell.h#L168-L180)

## Overview
CompoundAffixFlag is a structure that stores Hunspell options for compound word support, handling different flag representations based on the dictionary's flag encoding mode.

## Definition
```c
typedef struct CompoundAffixFlag
{
    union
    {
        /* Flag name if flagMode is FM_CHAR or FM_LONG */
        char       *s;
        /* Flag name if flagMode is FM_NUM */
        uint32      i;
    }           flag;
    /* we don't have a bsearch_arg version, so, copy FlagMode */
    FlagMode    flagMode;
    uint32      value;
} CompoundAffixFlag;
```

## Detailed Description
CompoundAffixFlag represents configuration options specific to compound word processing in PostgreSQL's Ispell dictionary system, providing compatibility with Hunspell dictionary formats. This structure stores affix flags that control various aspects of compound word formation and validation. The design accommodates different flag representation formats used by Hunspell dictionaries: single character flags (FM_CHAR), two-character flags (FM_LONG), and numeric flags (FM_NUM).

The union-based flag storage optimizes memory usage while supporting the different encoding schemes, and the structure includes both the flag identifier and its associated processing value. These flags are essential for implementing advanced spell checking features like compound word recognition, word breaking, and morphological analysis.

## Parameters / Member Variables
- `flag`: Union containing the flag identifier in different formats
  - `s`: String representation for FM_CHAR and FM_LONG flag modes
  - `i`: Integer representation for FM_NUM flag mode
- `flagMode`: Enumeration indicating the flag encoding type (FM_CHAR, FM_LONG, or FM_NUM)
- `value`: Associated processing value or parameter for this compound affix flag

## Dependencies
- Functions called/Symbols referenced:
  - FlagMode (enumeration for flag encoding types)
- Called from (representative examples):
  - addCompoundAffixFlagValue (creates and adds new CompoundAffixFlag entries)
  - setCompoundAffixFlagValue (initializes CompoundAffixFlag structure fields)
  - getCompoundAffixFlagValue (retrieves flag values during processing)
  - cmpcmdflag (comparison function for sorting CompoundAffixFlag arrays)
  - NIImportOOAffixes (processes compound flags during affix import)

## Notes and Other Information
- Part of PostgreSQL's Ispell dictionary implementation located in src/include/tsearch/dicts/spell.h:168-180
- Stored in IspellDict.CompoundAffixFlags array during dictionary construction
- Supports Hunspell compatibility for advanced compound word processing
- Flag encoding modes: FM_CHAR (single character), FM_LONG (two characters), FM_NUM (numeric 0-65535)
- Essential for implementing compound word validation and morphological analysis
- Used during dictionary initialization and remains available throughout dictionary lifetime
- Memory managed through PostgreSQL's temporary allocation system during construction phase