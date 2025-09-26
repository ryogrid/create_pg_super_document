# aff_struct (AFFIX)

## Overview
aff_struct is a structure that represents an entry in an affix list for PostgreSQL's Ispell dictionary system, storing affix rules including patterns for word prefixes and suffixes.

## Definition
```c
typedef struct aff_struct
{
    char       *flag;
    /* FF_SUFFIX or FF_PREFIX */
    uint32      type:1,
                flagflags:7,
                issimple:1,
                isregis:1,
                replen:14;
    char       *find;
    char       *repl;
    union
    {
        /*
         * Arrays of AFFIX are moved and sorted.  We'll use a pointer to
         * regex_t to keep this struct small, and avoid assuming that regex_t
         * is movable.
         */
        regex_t    *pregex;
        Regis       regis;
    }           reg;
} AFFIX;
```

## Detailed Description
The aff_struct (typedef'd as AFFIX) represents affix rules in PostgreSQL's Ispell dictionary implementation, storing both prefix and suffix transformation rules. Each AFFIX entry describes how to transform a base word by specifying what to remove (find) and what to add (repl), along with conditions that determine when the rule applies. The structure supports three different pattern matching mechanisms: simple string matching for basic cases, Regis-based matching for complex patterns, and POSIX regex for the most general cases.

The bitfield design optimizes memory usage while providing flags for compound word handling, rule type specification (prefix/suffix), and pattern matching strategy selection. This structure is essential for implementing Hunspell-compatible affix processing in PostgreSQL's text search functionality.

## Parameters / Member Variables
- `flag`: String identifier for this affix rule, used for grouping and referencing
- `type`: Single bit indicating FF_SUFFIX (1) or FF_PREFIX (0)
- `flagflags`: 7-bit field storing compound word and other processing flags
- `issimple`: Single bit indicating whether this rule uses simple string matching
- `isregis`: Single bit indicating whether this rule uses Regis pattern matching
- `replen`: 14-bit field storing the length of the replacement string
- `find`: String pattern to remove from the word (stripping pattern)
- `repl`: String to add after stripping (replacement pattern)
- `reg`: Union containing either regex_t pointer for POSIX regex or Regis structure for custom pattern matching

## Dependencies
- Functions called/Symbols referenced:
  - regex_t (POSIX regular expression structure)
  - [Regis](../R/Regis.md) (custom pattern matching structure)
- Called from (representative examples):
  - [NIAddAffix](../N/NIAddAffix.md) (creates and initializes new AFFIX entries)
  - [cmpaffix](../c/cmpaffix.md) (comparison function for sorting AFFIX entries)
  - [mkANode](../m/mkANode.md) (uses AFFIX data to build affix tree nodes)
  - [CheckAffix](../C/CheckAffix.md) (validates and applies affix rules)
  - [NISortAffixes](../N/NISortAffixes.md) (sorts affix arrays for efficient processing)

## Notes and Other Information
- Part of PostgreSQL's Ispell dictionary implementation located in src/include/tsearch/dicts/spell.h:87-107
- Compatible with Hunspell affix file format and processing rules
- The union design allows efficient memory usage for different pattern matching strategies
- Used extensively in affix tree construction for efficient rule application
- Supports compound word processing through flagflags field
- Memory allocated using PostgreSQL's memory context system for automatic cleanup