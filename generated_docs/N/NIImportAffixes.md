# NIImportAffixes

## Location
[src/backend/tsearch/spell.c:1428-1574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1428-L1574)

## Overview
Parses ispell affix files, automatically detecting and handling both old-format (ispell) and new-format (MySpell/Hunspell) affix files.

## Definition
```c
void NIImportAffixes(IspellDict *Conf, const char *filename)
```

## Detailed Description
This function serves as the main entry point for affix file parsing. It initially attempts to parse files in the old ispell format, which uses simple keywords like "suffixes", "prefixes", "flag", and "compoundwords". When it encounters new-format commands (COMPOUNDFLAG, COMPOUNDMIN, PFX, SFX), it delegates the parsing to NIImportOOAffixes(). The function supports compound word processing through the "compoundwords" directive and handles cross-product flags (*) and compound-only flags (~). It ensures that files don't mix old and new format commands.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure to be configured with affix data
- `filename`: Path to the affix file (caller must have applied get_tsearch_config_filename)

## Dependencies
- Functions called/Symbols referenced:
  - [tsearch_readline_begin](../t/tsearch_readline_begin.md)/tsearch_readline/tsearch_readline_end (file reading utilities)
  - [lowerstr](../l/lowerstr.md) (string case conversion)
  - [findchar2](../f/findchar2.md) (character search utility)
  - [addCompoundAffixFlagValue](../a/addCompoundAffixFlagValue.md) (compound flag processing)
  - [parse_affentry](../p/parse_affentry.md) (old-format affix entry parsing)
  - [NIAddAffix](NIAddAffix.md) (adds affix to dictionary)
  - [NIImportOOAffixes](NIImportOOAffixes.md) (new-format affix processing)
- Called from (representative examples):
  - [dispell_init](../d/dispell_init.md) (dictionary initialization)
  - IspellDict (dictionary setup)

## Notes and Other Information
- Automatically detects file format by parsing initial commands
- Old format keywords: "suffixes", "prefixes", "flag", "compoundwords"
- New format keywords: "COMPOUNDFLAG", "COMPOUNDMIN", "PFX", "SFX"
- Supports flag modifiers: * (cross-product), ~ (compound-only)
- Throws error if file mixes old and new format commands
- Old format uses single ASCII character flags, new format supports various flag modes
- Function re-reads entire file when switching to new-format parsing