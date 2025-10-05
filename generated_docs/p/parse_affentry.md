# parse_affentry

## Location
[src/backend/tsearch/spell.c:914-1031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L914-L1031)

## Overview
Parses a single entry from an Ispell format .affix file, extracting the mask, find pattern, and replacement pattern components used for morphological analysis.

## Definition
```c
static bool parse_affentry(char *str, char *mask, char *find, char *repl)
```

## Detailed Description
This function implements a finite state machine to parse entries from Ispell-format affix files. Each affix entry follows the format: `<mask> > [-<find>,]<replace>`. The function processes the input string character by character, transitioning between different parsing states:

- PAE_WAIT_MASK: Waiting for the mask portion
- PAE_INMASK: Reading the mask characters
- PAE_WAIT_FIND: Waiting for the find pattern after '>'
- PAE_INFIND: Reading the find pattern (after '-')
- PAE_WAIT_REPL: Waiting for the replacement pattern
- PAE_INREPL: Reading the replacement pattern

The parser handles multibyte characters properly and validates syntax, reporting errors for malformed entries. Comments starting with '#' are ignored.

## Parameters / Member Variables
- `str`: Input string containing the affix entry to parse
- `mask`: Output buffer to store the extracted mask pattern
- `find`: Output buffer to store the find pattern (what to remove)
- `repl`: Output buffer to store the replacement pattern (what to add)

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq: Character comparison for text search
  - [t_isspace](../t/t_isspace.md): Space character testing
  - [t_isalpha](../t/t_isalpha.md): Alphabetic character testing
  - COPYCHAR: Macro for copying multibyte characters
  - [pg_mblen](pg_mblen.md): Get multibyte character length
  - ereport/elog: Error reporting functions
- Called from (representative examples):
  - [NIImportAffixes](../N/NIImportAffixes.md): Imports affixes from configuration files

## Notes and Other Information
- Returns true if parsing was successful and at least mask and (find or repl) are non-empty
- Uses a state machine approach for robust parsing of complex affix syntax
- Handles multibyte character encodings properly through pg_mblen
- Part of PostgreSQL's full-text search (tsearch) spell-checking functionality
- Supports Ispell dictionary format for morphological analysis

## Simplified Source

```c
static bool parse_affentry(char *str, char *mask, char *find, char *repl) {
    int state = PAE_WAIT_MASK;
    char *pmask = mask, *pfind = find, *prepl = repl;

    // Initialize output buffers
    *mask = *find = *repl = '\0';

    while (*str) {
        switch (state) {
            case PAE_WAIT_MASK:
                // Skip comments starting with #
                if (t_iseq(str, '#'))
                    return false;
                // Start reading mask when non-space found
                if (!t_isspace(str)) {
                    COPYCHAR(pmask, str);
                    pmask += pg_mblen(str);
                    state = PAE_INMASK;
                }
                break;

            case PAE_INMASK:
                // End of mask marked by '>'
                if (t_iseq(str, '>')) {
                    *pmask = '\0';
                    state = PAE_WAIT_FIND;
                } else if (!t_isspace(str)) {
                    // Continue reading mask
                    COPYCHAR(pmask, str);
                    pmask += pg_mblen(str);
                }
                break;

            case PAE_WAIT_FIND:
                // '-' indicates start of find pattern
                if (t_iseq(str, '-')) {
                    state = PAE_INFIND;
                } else if (t_isalpha(str) || t_iseq(str, '\'')) {
                    // Direct replacement without find pattern
                    COPYCHAR(prepl, str);
                    prepl += pg_mblen(str);
                    state = PAE_INREPL;
                } else if (!t_isspace(str)) {
                    ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                                  errmsg("syntax error")));
                }
                break;

            case PAE_INFIND:
                // ',' ends find pattern, starts replacement
                if (t_iseq(str, ',')) {
                    *pfind = '\0';
                    state = PAE_WAIT_REPL;
                } else if (t_isalpha(str)) {
                    COPYCHAR(pfind, str);
                    pfind += pg_mblen(str);
                }
                break;

            case PAE_WAIT_REPL:
                // Start reading replacement pattern
                if (t_iseq(str, '-')) {
                    break; // Empty replacement
                } else if (t_isalpha(str)) {
                    COPYCHAR(prepl, str);
                    prepl += pg_mblen(str);
                    state = PAE_INREPL;
                }
                break;

            case PAE_INREPL:
                // '#' ends replacement (comment follows)
                if (t_iseq(str, '#')) {
                    *prepl = '\0';
                    goto done;
                } else if (t_isalpha(str)) {
                    COPYCHAR(prepl, str);
                    prepl += pg_mblen(str);
                }
                break;
        }
        str += pg_mblen(str);
    }

done:
    *pmask = *pfind = *prepl = '\0';
    return (*mask && (*find || *repl));
}
```