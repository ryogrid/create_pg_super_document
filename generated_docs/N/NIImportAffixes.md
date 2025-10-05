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

## Simplified Source

```c
void NIImportAffixes(IspellDict *Conf, const char *filename) {
    char *pstr = NULL;
    char flag[BUFSIZ], mask[BUFSIZ], find[BUFSIZ], repl[BUFSIZ];
    char *s;
    bool suffixes = false, prefixes = false;
    char flagflags = 0;
    tsearch_readline_state trst;
    bool oldformat = false;
    char *recoded = NULL;

    // Initialize configuration
    if (!tsearch_readline_begin(&trst, filename))
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                      errmsg("could not open affix file \"%s\": %m", filename)));

    Conf->usecompound = false;
    Conf->useFlagAliases = false;
    Conf->flagMode = FM_CHAR;

    // Parse file line by line
    while ((recoded = tsearch_readline(&trst)) != NULL) {
        pstr = lowerstr(recoded);

        // Skip comments and empty lines
        if (*pstr == '#' || *pstr == '\n')
            goto nextline;

        // Handle compound words directive
        if (STRNCMP(pstr, "compoundwords") == 0) {
            s = findchar2(recoded, 'l', 'L');
            if (s) {
                // Skip to flag character
                while (*s && !t_isspace(s)) s += pg_mblen(s);
                while (*s && t_isspace(s)) s += pg_mblen(s);

                if (*s && pg_mblen(s) == 1) {
                    addCompoundAffixFlagValue(Conf, s, FF_COMPOUNDFLAG);
                    Conf->usecompound = true;
                }
                oldformat = true;
                goto nextline;
            }
        }

        // Handle section headers
        if (STRNCMP(pstr, "suffixes") == 0) {
            suffixes = true; prefixes = false; oldformat = true;
            goto nextline;
        }
        if (STRNCMP(pstr, "prefixes") == 0) {
            suffixes = false; prefixes = true; oldformat = true;
            goto nextline;
        }

        // Handle flag definitions
        if (STRNCMP(pstr, "flag") == 0) {
            s = recoded + 4;  // Skip "flag"
            flagflags = 0;

            while (*s && t_isspace(s)) s += pg_mblen(s);

            // Handle flag modifiers
            if (*s == '*') {
                flagflags |= FF_CROSSPRODUCT;
                s++;
            } else if (*s == '~') {
                flagflags |= FF_COMPOUNDONLY;
                s++;
            }

            if (*s == '\\') s++;

            // Check for old-format single character flag
            if (*s && pg_mblen(s) == 1) {
                COPYCHAR(flag, s);
                flag[1] = '\0';
                s++;

                if (*s == '\0' || *s == '#' || *s == '\n' || *s == ':' || t_isspace(s)) {
                    oldformat = true;
                    goto nextline;
                }
            }
            goto isnewformat;
        }

        // Check for new-format commands
        if (STRNCMP(recoded, "COMPOUNDFLAG") == 0 ||
            STRNCMP(recoded, "COMPOUNDMIN") == 0 ||
            STRNCMP(recoded, "PFX") == 0 ||
            STRNCMP(recoded, "SFX") == 0)
            goto isnewformat;

        // Process affix entries (only if in suffix/prefix section)
        if ((!suffixes) && (!prefixes))
            goto nextline;

        if (!parse_affentry(pstr, mask, find, repl))
            goto nextline;

        NIAddAffix(Conf, flag, flagflags, mask, find, repl,
                  suffixes ? FF_SUFFIX : FF_PREFIX);

nextline:
        pfree(recoded);
        pfree(pstr);
    }
    tsearch_readline_end(&trst);
    return;

isnewformat:
    // Error if mixing old and new formats
    if (oldformat)
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                      errmsg("affix file contains both old-style and new-style commands")));
    tsearch_readline_end(&trst);

    // Delegate to new-format parser
    NIImportOOAffixes(Conf, filename);
}
```