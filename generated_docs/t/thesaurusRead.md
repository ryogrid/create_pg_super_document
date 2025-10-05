# thesaurusRead

## Location
[src/backend/tsearch/dict_thesaurus.c:168-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L168-L302)

## Overview
Parses a thesaurus configuration file and populates the DictThesaurus structure with lexeme patterns and their replacement words.

## Definition
```c
static void thesaurusRead(const char *filename, DictThesaurus *d)
```

## Detailed Description
The thesaurusRead function implements a finite state machine parser for thesaurus configuration files. It processes each line of the file to extract substitution rules in the format "pattern_words : replacement_words". The parser handles four distinct states: waiting for lexemes (TR_WAITLEX), inside a lexeme (TR_INLEX), waiting for substitutions (TR_WAITSUBS), and inside a substitution word (TR_INSUBS).

The function supports special syntax including comments (lines starting with #), use-as-is markers (*word), and escaped words (\word). Each substitution rule is assigned a unique ID and stored in the thesaurus dictionary with both the pattern lexemes and replacement words properly indexed for efficient lookup during text search operations.

## Parameters / Member Variables
- `filename`: Path to the thesaurus configuration file to be parsed
- `d`: Pointer to the DictThesaurus structure that will be populated with the parsed rules

## Dependencies
- Functions called/Symbols referenced:
  - [get_tsearch_config_filename](../g/get_tsearch_config_filename.md): Resolves the full path to the thesaurus file with .ths extension
  - [tsearch_readline_begin](tsearch_readline_begin.md)/tsearch_readline/tsearch_readline_end: File reading utilities for text search configurations
  - [newLexeme](../n/newLexeme.md): Creates lexeme entries for pattern words
  - [addWrd](../a/addWrd.md): Adds replacement words to substitution rules
  - [t_isspace](t_isspace.md)/t_iseq: Text processing utilities for Unicode-aware character testing
  - [pg_mblen](../p/pg_mblen.md): PostgreSQL multibyte character length function
  - ereport/elog: PostgreSQL error reporting functions
  - [pfree](../p/pfree.md): PostgreSQL memory deallocation function

- Called from (representative examples):
  - [thesaurus_init](thesaurus_init.md): Dictionary initialization function that loads the thesaurus configuration

## Notes and Other Information
- This is a static function, only accessible within the dict_thesaurus.c file
- Implements a finite state machine with four states to parse the thesaurus syntax
- Supports Unicode text through PostgreSQL's multibyte character functions
- Provides detailed error reporting for malformed configuration files
- The parser expects the format: "word1 word2 : replacement1 replacement2"
- Special prefixes: "*" for use-as-is processing, "\" for escaped words
- Comments and blank lines are automatically skipped
- Each substitution rule gets a unique sequential ID starting from 0
- Validates that substitution rules are not empty and don't exceed size limits

## Simplified Source

```c
static void
thesaurusRead(const char *filename, DictThesaurus *d)
{
    tsearch_readline_state trst;
    uint32 idsubst = 0;
    bool useasis = false;
    char *line;

    // Open thesaurus file
    filename = get_tsearch_config_filename(filename, "ths");
    if (!tsearch_readline_begin(&trst, filename))
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("could not open thesaurus file \"%s\": %m", filename)));

    // Parse each line using finite state machine
    while ((line = tsearch_readline(&trst)) != NULL)
    {
        char *ptr;
        int state = TR_WAITLEX;  // Start waiting for lexemes
        char *beginwrd = NULL;
        uint32 posinsubst = 0;
        uint32 nwrd = 0;

        ptr = line;

        // Skip comments and empty lines
        while (*ptr && t_isspace(ptr))
            ptr += pg_mblen(ptr);
        if (t_iseq(ptr, '#') || *ptr == '\0' || t_iseq(ptr, '\n') || t_iseq(ptr, '\r'))
        {
            pfree(line);
            continue;
        }

        // Parse line character by character
        while (*ptr)
        {
            if (state == TR_WAITLEX)
            {
                if (t_iseq(ptr, ':'))
                {
                    if (posinsubst == 0)
                        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                                errmsg("unexpected delimiter")));
                    state = TR_WAITSUBS;
                }
                else if (!t_isspace(ptr))
                {
                    beginwrd = ptr;
                    state = TR_INLEX;
                }
            }
            else if (state == TR_INLEX)
            {
                if (t_iseq(ptr, ':'))
                {
                    newLexeme(d, beginwrd, ptr, idsubst, posinsubst++);
                    state = TR_WAITSUBS;
                }
                else if (t_isspace(ptr))
                {
                    newLexeme(d, beginwrd, ptr, idsubst, posinsubst++);
                    state = TR_WAITLEX;
                }
            }
            else if (state == TR_WAITSUBS)
            {
                // Check for special markers: * (useasis) or \ (escape)
                if (t_iseq(ptr, '*'))
                {
                    useasis = true;
                    state = TR_INSUBS;
                    beginwrd = ptr + pg_mblen(ptr);
                }
                else if (t_iseq(ptr, '\\'))
                {
                    useasis = false;
                    state = TR_INSUBS;
                    beginwrd = ptr + pg_mblen(ptr);
                }
                else if (!t_isspace(ptr))
                {
                    useasis = false;
                    beginwrd = ptr;
                    state = TR_INSUBS;
                }
            }
            else if (state == TR_INSUBS)
            {
                if (t_isspace(ptr))
                {
                    if (ptr == beginwrd)
                        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                                errmsg("unexpected end of line or lexeme")));
                    addWrd(d, beginwrd, ptr, idsubst, nwrd++, posinsubst, useasis);
                    state = TR_WAITSUBS;
                }
            }

            ptr += pg_mblen(ptr);
        }

        // Handle end of line in TR_INSUBS state
        if (state == TR_INSUBS)
        {
            if (ptr == beginwrd)
                ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("unexpected end of line or lexeme")));
            addWrd(d, beginwrd, ptr, idsubst, nwrd++, posinsubst, useasis);
        }

        idsubst++;

        // Validate the rule has both pattern and substitution
        if (!(nwrd && posinsubst))
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("unexpected end of line")));

        // Check for overflow
        if (nwrd != (uint16) nwrd || posinsubst != (uint16) posinsubst)
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("too many lexemes in thesaurus entry")));

        pfree(line);
    }

    d->nsubst = idsubst;
    tsearch_readline_end(&trst);
}
```