# SPELL

## Location
src/include/tsearch/dicts/spell.h: 80 - 81

## Overview
SPELL is a structure that represents an entry in a words list for the ISpell dictionary system in PostgreSQL's text search functionality.

## Definition


## Detailed Description
The SPELL structure represents a single word entry in an ISpell dictionary. It uses a union to optimize memory usage during different phases of dictionary processing. During dictionary import, the  field contains affix information as a string. After sorting via NISortDictionary(), the structure switches to using the  substructure which contains numeric references to affix data and the word length for more efficient processing.

The structure uses a flexible array member for the  field, allowing it to store words of varying lengths without wasting memory. This design is part of PostgreSQL's text search system that provides spell checking and word normalization capabilities compatible with ISpell/Hunspell dictionaries.

## Parameters / Member Variables
- : Union containing either flag string or processed data
  - : String containing affix flags (used during import)
  - : Structure used after sorting containing:
    - : Numeric reference to an entry in the AffixData field
    - : Length of the word in characters
- : Flexible array member storing the actual word characters

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro)
- Called from (representative examples):
  - [cmpspell](../c/cmpspell.md) (comparison function for sorting)
  - [cmpspellaffix](../c/cmpspellaffix.md) (comparison function for affix sorting)
  - [NIAddSpell](../N/NIAddSpell.md) (adds spell entries to dictionary)
  - [NISortDictionary](../N/NISortDictionary.md) (sorts dictionary entries)

## Notes and Other Information
- The SPELLHDRSZ macro defines the header size offset for this structure
- Memory layout is optimized for both import and runtime phases of dictionary processing
- Part of PostgreSQL's full-text search implementation supporting ISpell/Hunspell dictionary formats
- The union design allows the same memory footprint to serve different purposes during dictionary lifecycle