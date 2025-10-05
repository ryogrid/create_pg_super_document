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
  - [tsearch_readline_begin](../t/tsearch_readline_begin.md)/tsearch_readline/tsearch_readline_end (file reading utilities)
  - [addCompoundAffixFlagValue](../a/addCompoundAffixFlagValue.md) (processes compound flags)
  - [parse_ooaffentry](../p/parse_ooaffentry.md) (parses individual affix entries)
  - [lowerstr_ctx](../l/lowerstr_ctx.md) (string case conversion)
  - [getCompoundAffixFlagValue](../g/getCompoundAffixFlagValue.md)/getAffixFlagSet (flag processing)
  - [NIAddAffix](NIAddAffix.md) (adds parsed affix to dictionary)
  - qsort/cmpcmdflag (sorts compound flags)
- Called from (representative examples):
  - [NIImportAffixes](NIImportAffixes.md) (main affix import function)

## Notes and Other Information
- Supports three flag modes: FM_CHAR (single character), FM_LONG (two characters), FM_NUM (numeric)
- Handles compound word flags: COMPOUNDFLAG, COMPOUNDBEGIN, COMPOUNDLAST, COMPOUNDMIDDLE, ONLYINCOMPOUND, COMPOUNDPERMITFLAG, COMPOUNDFORBIDFLAG
- Implements alias compression feature (AF parameter) to reduce memory usage
- Processes both prefix (PFX) and suffix (SFX) transformation rules
- Cross-product flag (FF_CROSSPRODUCT) allows combining prefixes and suffixes
- Error handling for invalid flag configurations and file access issues

## Simplified Source

```c
static void NIImportOOAffixes(IspellDict *Conf, const char *filename) {
    char type[BUFSIZ], *ptype = NULL;
    char sflag[BUFSIZ], mask[BUFSIZ], find[BUFSIZ], repl[BUFSIZ];
    bool isSuffix = false;
    int naffix = 0, curaffix = 0;
    char flagflags = 0;
    tsearch_readline_state trst;
    char *recoded;

    // Initialize configuration
    Conf->usecompound = false;
    Conf->useFlagAliases = false;
    Conf->flagMode = FM_CHAR;

    // First pass: read compound flags and configuration
    if (!tsearch_readline_begin(&trst, filename))
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                      errmsg("could not open affix file \"%s\": %m", filename)));

    while ((recoded = tsearch_readline(&trst)) != NULL) {
        // Skip empty lines and comments
        if (*recoded == '\0' || t_isspace(recoded) || t_iseq(recoded, '#')) {
            pfree(recoded);
            continue;
        }

        // Process compound flags
        if (STRNCMP(recoded, "COMPOUNDFLAG") == 0)
            addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDFLAG"), FF_COMPOUNDFLAG);
        else if (STRNCMP(recoded, "COMPOUNDBEGIN") == 0)
            addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUNDBEGIN"), FF_COMPOUNDBEGIN);
        else if (STRNCMP(recoded, "COMPOUNDLAST") == 0 || STRNCMP(recoded, "COMPOUNDEND") == 0)
            addCompoundAffixFlagValue(Conf, recoded + strlen("COMPOUND"), FF_COMPOUNDLAST);
        // ... other compound flags handled similarly

        // Process flag mode settings
        else if (STRNCMP(recoded, "FLAG") == 0) {
            char *s = recoded + strlen("FLAG");
            while (*s && t_isspace(s))
                s += pg_mblen(s);

            if (STRNCMP(s, "long") == 0)
                Conf->flagMode = FM_LONG;
            else if (STRNCMP(s, "num") == 0)
                Conf->flagMode = FM_NUM;
        }

        pfree(recoded);
    }
    tsearch_readline_end(&trst);

    // Sort compound flags for binary search
    if (Conf->nCompoundAffixFlag > 1)
        qsort(Conf->CompoundAffixFlags, Conf->nCompoundAffixFlag,
              sizeof(CompoundAffixFlag), cmpcmdflag);

    // Second pass: process affix rules
    if (!tsearch_readline_begin(&trst, filename))
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                      errmsg("could not open affix file \"%s\": %m", filename)));

    while ((recoded = tsearch_readline(&trst)) != NULL) {
        int fields_read;

        if (*recoded == '\0' || t_isspace(recoded) || t_iseq(recoded, '#'))
            goto nextline;

        fields_read = parse_ooaffentry(recoded, type, sflag, find, repl, mask);

        if (ptype) pfree(ptype);
        ptype = lowerstr_ctx(Conf, type);

        // Handle alias compression (AF parameter)
        if (STRNCMP(ptype, "af") == 0) {
            if (!Conf->useFlagAliases) {
                // Initialize alias array
                Conf->useFlagAliases = true;
                naffix = atoi(sflag) + 1;  // +1 for empty flag set
                Conf->AffixData = (char **) palloc0(naffix * sizeof(char *));
                Conf->AffixData[curaffix++] = VoidString;
            } else {
                // Add alias entry
                Conf->AffixData[curaffix++] = cpstrdup(Conf, sflag);
            }
            goto nextline;
        }

        // Process prefix/suffix rules
        if (fields_read >= 4 && (STRNCMP(ptype, "sfx") == 0 || STRNCMP(ptype, "pfx") == 0)) {
            if (fields_read == 4) {
                // Affix header
                isSuffix = (STRNCMP(ptype, "sfx") == 0);
                flagflags = (t_iseq(find, 'y') || t_iseq(find, 'Y')) ? FF_CROSSPRODUCT : 0;
            } else {
                // Affix rule entry
                char *ptr;
                int aflg = 0;

                // Process compound flags after '/'
                if ((ptr = strchr(repl, '/')) != NULL)
                    aflg |= getCompoundAffixFlagValue(Conf, getAffixFlagSet(Conf, ptr + 1));

                // Convert to lowercase and clean up
                char *prepl = lowerstr_ctx(Conf, repl);
                char *pfind = lowerstr_ctx(Conf, find);
                char *pmask = lowerstr_ctx(Conf, mask);

                if ((ptr = strchr(prepl, '/')) != NULL) *ptr = '\0';
                if (t_iseq(find, '0')) *pfind = '\0';
                if (t_iseq(repl, '0')) *prepl = '\0';

                // Add affix to dictionary
                NIAddAffix(Conf, sflag, flagflags | aflg, pmask, pfind, prepl,
                          isSuffix ? FF_SUFFIX : FF_PREFIX);

                pfree(prepl);
                pfree(pfind);
                pfree(pmask);
            }
        }

nextline:
        pfree(recoded);
    }

    tsearch_readline_end(&trst);
    if (ptype) pfree(ptype);
}
```