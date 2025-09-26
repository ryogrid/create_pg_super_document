# spell_struct (SPELL)

## Overview
spell_struct is a structure that represents an entry in a words list for PostgreSQL's Ispell dictionary system, storing individual words along with their associated affix flags and metadata.

## Definition
```c
typedef struct spell_struct
{
    union
    {
        /*
         * flag is filled in by NIImportDictionary(). After
         * NISortDictionary(), d is used instead of flag.
         */
        char       *flag;
        /* d is used in mkSPNode() */
        struct
        {
            /* Reference to an entry of the AffixData field */
            int         affix;
            /* Length of the word */
            int         len;
        }           d;
    }           p;
    char        word[FLEXIBLE_ARRAY_MEMBER];
} SPELL;
```

## Detailed Description
The spell_struct (typedef'd as SPELL) serves as the fundamental data structure for storing dictionary words during the construction and operation of Ispell dictionaries in PostgreSQL's text search system. This structure undergoes a transformation during the dictionary building process: initially, the flag field stores affix flags as strings imported from dictionary files, but after sorting via NISortDictionary(), the structure switches to using the d sub-structure which contains processed affix references and word length for efficient runtime operations.

The design uses a union to optimize memory usage, allowing the same memory space to serve different purposes during different phases of dictionary processing. The flexible array member for the word field allows efficient storage of variable-length words without additional pointer indirection.

## Parameters / Member Variables
- `p`: Union containing either import-time flags or runtime-optimized data
  - `flag`: String containing affix flags during dictionary import phase
  - `d.affix`: Integer reference to AffixData entry after processing
  - `d.len`: Length of the stored word after processing
- `word`: Flexible array containing the actual dictionary word as a null-terminated string

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array implementation)
- Called from (representative examples):
  - NIAddSpell (adds new words to temporary SPELL array)
  - cmpspell (comparison function for sorting SPELL entries)
  - cmpspellaffix (comparison function considering affix data)
  - NISortDictionary (processes and sorts SPELL array)
  - mkSPNode (uses SPELL data to build prefix tree)

## Notes and Other Information
- Part of PostgreSQL's Ispell dictionary implementation located in src/include/tsearch/dicts/spell.h:61-79
- The SPELLHDRSZ macro calculates the header size for memory allocation
- Used extensively during dictionary construction phase in IspellDict.Spell array
- Memory layout optimized for both import phase (string flags) and runtime phase (integer references)
- Essential component in building the SPNode prefix tree structure for efficient word lookups