# NIAddAffix

## Location
src/backend/tsearch/spell.c: 678 - 771

## Overview
Adds a new affix rule to the dictionary's Affix array, handling pattern compilation and memory management for prefix/suffix transformation rules.

## Definition
```c
static void NIAddAffix(IspellDict *Conf, const char *flag, char flagflags, const char *mask, const char *find, const char *repl, int type)
```

## Detailed Description
NIAddAffix creates and initializes a new AFFIX structure in the dictionary's affix array. The function handles dynamic memory allocation, expanding the affix array when necessary. It processes three types of pattern matching for word endings:

1. **Simple patterns**: When mask is "." or empty, applies to all words
2. **Regis patterns**: Uses PostgreSQL's simplified regex engine for basic patterns
3. **Full regex patterns**: Compiles complex regular expressions using pg_regcomp

The function automatically manages compound word flags, ensuring that words marked with FF_COMPOUNDONLY or FF_COMPOUNDPERMITFLAG also have the FF_COMPOUNDFLAG set. It uses the dictionary's memory context for all allocations, ensuring proper cleanup when the dictionary is destroyed.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing the dictionary configuration
- `flag`: Single character affix flag identifier (e.g., 'S', 'M', 'L')
- `flagflags`: Set of flags from the flagval field controlling affix behavior
- `mask`: Condition pattern for word endings (e.g., '[^Y]', '.', '^pre')
- `find`: Characters to strip from word beginning (prefix) or end (suffix), '0' means no stripping
- `repl`: String to add after stripping the 'find' characters
- `type`: Either FF_SUFFIX or FF_PREFIX to indicate affix type

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md)
  - [palloc](../p/palloc.md)
  - [RS_isRegis](../R/RS_isRegis.md)
  - [RS_compile](../R/RS_compile.md)
  - tmpalloc
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md)
  - pg_regcomp
  - [pg_regerror](../p/pg_regerror.md)
  - [cpstrdup](../c/cpstrdup.md)
  - FF_SUFFIX
  - FF_PREFIX
  - FF_COMPOUNDONLY
  - FF_COMPOUNDPERMITFLAG
  - FF_COMPOUNDFLAG
- Called from (representative examples):
  - NIImportOOAffixes
  - NIImportAffixes

## Notes and Other Information
- Static function, only accessible within the spell.c module
- Dynamically expands the Affix array, starting with 16 entries and doubling when full
- Automatically adds compound flags when compound-related flags are present
- Uses VoidString constant for empty find/repl strings to save memory
- Regex compilation errors are reported with appropriate PostgreSQL error codes
- Memory is allocated in the dictionary's context for automatic cleanup
- Supports both simple string matching and complex regex patterns for word ending conditions