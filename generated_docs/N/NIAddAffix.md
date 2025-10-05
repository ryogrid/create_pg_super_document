# NIAddAffix

## Location
[src/backend/tsearch/spell.c:678-771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L678-L771)

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
  - [NIImportOOAffixes](NIImportOOAffixes.md)
  - [NIImportAffixes](NIImportAffixes.md)

## Notes and Other Information
- Static function, only accessible within the spell.c module
- Dynamically expands the Affix array, starting with 16 entries and doubling when full
- Automatically adds compound flags when compound-related flags are present
- Uses VoidString constant for empty find/repl strings to save memory
- Regex compilation errors are reported with appropriate PostgreSQL error codes
- Memory is allocated in the dictionary's context for automatic cleanup
- Supports both simple string matching and complex regex patterns for word ending conditions

## Simplified Source

```c
static void
NIAddAffix(IspellDict *Conf, const char *flag, char flagflags, const char *mask,
           const char *find, const char *repl, int type)
{
    AFFIX *Affix;

    // Expand affix array if needed
    if (Conf->naffixes >= Conf->maffixes) {
        if (Conf->maffixes) {
            Conf->maffixes *= 2;
            Conf->Affix = (AFFIX *) repalloc(Conf->Affix,
                                             Conf->maffixes * sizeof(AFFIX));
        } else {
            Conf->maffixes = 16;
            Conf->Affix = (AFFIX *) palloc(Conf->maffixes * sizeof(AFFIX));
        }
    }

    Affix = Conf->Affix + Conf->naffixes;

    // Determine pattern matching method
    if (strcmp(mask, ".") == 0 || *mask == '\0') {
        // Simple: matches any word ending
        Affix->issimple = 1;
        Affix->isregis = 0;
    } else if (RS_isRegis(mask)) {
        // Use simpler regex engine for basic patterns
        Affix->issimple = 0;
        Affix->isregis = 1;
        RS_compile(&(Affix->reg.regis), (type == FF_SUFFIX),
                   *mask ? mask : VoidString);
    } else {
        // Use full regex engine for complex patterns
        Affix->issimple = 0;
        Affix->isregis = 0;

        // Build regex pattern with anchors
        char *tmask = (char *) tmpalloc(strlen(mask) + 3);
        if (type == FF_SUFFIX)
            sprintf(tmask, "%s$", mask);  // Anchor to end
        else
            sprintf(tmask, "^%s", mask);  // Anchor to start

        // Convert to wide chars and compile regex
        int masklen = strlen(tmask);
        pg_wchar *wmask = (pg_wchar *) tmpalloc((masklen + 1) * sizeof(pg_wchar));
        int wmasklen = pg_mb2wchar_with_len(tmask, wmask, masklen);

        Affix->reg.pregex = palloc(sizeof(regex_t));
        int err = pg_regcomp(Affix->reg.pregex, wmask, wmasklen,
                             REG_ADVANCED | REG_NOSUB, DEFAULT_COLLATION_OID);
        if (err)
            ereport(ERROR, /* regex compilation error */);
    }

    // Set affix properties
    Affix->flagflags = flagflags;

    // Auto-add compound flag if needed
    if ((flagflags & FF_COMPOUNDONLY) || (flagflags & FF_COMPOUNDPERMITFLAG)) {
        if ((flagflags & FF_COMPOUNDFLAG) == 0)
            Affix->flagflags |= FF_COMPOUNDFLAG;
    }

    Affix->flag = cpstrdup(Conf, flag);
    Affix->type = type;
    Affix->find = (find && *find) ? cpstrdup(Conf, find) : VoidString;

    if ((Affix->replen = strlen(repl)) > 0)
        Affix->repl = cpstrdup(Conf, repl);
    else
        Affix->repl = VoidString;

    Conf->naffixes++;
}
```