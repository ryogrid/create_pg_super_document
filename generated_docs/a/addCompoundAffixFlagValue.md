# addCompoundAffixFlagValue

## Location
[src/backend/tsearch/spell.c:1068-1124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1068-L1124)

## Overview
Adds a new compound affix flag and its associated value to the dictionary's compound affix flag array, managing dynamic memory allocation as needed.

## Definition
```c
static void addCompoundAffixFlagValue(IspellDict *Conf, char *s, uint32 val)
```

## Detailed Description
This function processes a string containing an affix flag, extracts the flag portion (excluding whitespace and newlines), and adds it to the dictionary's collection of compound affix flags. The function handles dynamic memory management by growing the CompoundAffixFlags array when needed:

- Initially allocates space for 10 flags using tmpalloc
- Doubles the array size using repalloc when more space is needed
- Parses the input string to extract the flag, skipping leading whitespace and stopping at whitespace or newline characters
- Uses setCompoundAffixFlagValue to properly configure the new flag entry
- Sets the dictionary's usecompound flag to true to indicate compound processing is enabled

## Parameters / Member Variables
- `Conf`: Pointer to the IspellDict configuration to modify
- `s`: String containing the affix flag to add (may include whitespace)
- `val`: Numeric value to associate with this compound affix flag

## Dependencies
- Functions called/Symbols referenced:
  - [t_isspace](../t/t_isspace.md): Text search space character testing
  - [pg_mblen](../p/pg_mblen.md): Get multibyte character length
  - COPYCHAR: Macro for copying multibyte characters
  - [repalloc](../r/repalloc.md): PostgreSQL memory reallocation function
  - tmpalloc: PostgreSQL temporary memory allocation
  - [setCompoundAffixFlagValue](../s/setCompoundAffixFlagValue.md): Configure the flag entry with parsed values
  - ereport: Error reporting for syntax errors
- Called from (representative examples):
  - NIImportOOAffixes: Import OpenOffice-style affix configurations
  - NIImportAffixes: Import standard Ispell affix configurations

## Notes and Other Information
- Automatically enables compound word processing by setting usecompound to true
- Handles multibyte character encodings properly throughout parsing
- Uses PostgreSQL's memory management functions for efficient allocation
- Part of the Hunspell-compatible spell checking system
- Grows the flag array exponentially to minimize reallocation overhead
- Validates input format and reports syntax errors for malformed flags