# dsynonym_init

## Location
[src/backend/tsearch/dict_synonym.c:92-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_synonym.c#L92-L209)

## Overview
Initializes a synonym dictionary by parsing a configuration file and building an internal data structure for efficient synonym lookups.

## Definition

```c
Datum
dsynonym_init(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the initialization entry point for PostgreSQL's synonym dictionary. It processes configuration parameters, reads a synonym file, and constructs a sorted array of synonym mappings for efficient lookup during text search operations.

The function performs these key operations:
1. Parses dictionary options (synonyms file path, case sensitivity)
2. Opens and reads the specified synonym file line by line
3. Extracts word pairs from each line using findwrd()
4. Builds an array of Syn structures containing input/output word mappings
5. Sorts the array for efficient binary search during lexicalization
6. Handles case sensitivity options and prefix flags

The synonym file format expects each line to contain an input word followed by its replacement word, separated by whitespace.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - dictoptions: List of configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md), List, ListCell (PostgreSQL list structures)
  - [defGetString](defGetString.md), defGetBoolean (configuration parsing)
  - [get_tsearch_config_filename](../g/get_tsearch_config_filename.md) (file path resolution)
  - [tsearch_readline_begin](../t/tsearch_readline_begin.md), tsearch_readline, tsearch_readline_end (file reading)
  - [findwrd](../f/findwrd.md) (word parsing)
  - [palloc0](../p/palloc0.md), repalloc, pstrdup, lowerstr (memory and string management)
  - qsort with compareSyn (array sorting)
- Called from (representative examples):
  - PostgreSQL dictionary initialization system (no direct callers in provided data)

## Notes and Other Information
- This is a PostgreSQL function callable from SQL for dictionary creation
- Supports two configuration parameters: 'synonyms' (required file path) and 'casesensitive' (optional boolean)
- Dynamically grows the synonym array as needed, starting with 64 entries and doubling when full
- Case insensitive mode converts all words to lowercase for consistent matching
- Ignores empty lines and lines with only one word in the synonym file
- The resulting dictionary structure is optimized for fast binary search lookups

## Simplified Source

```c
Datum
dsynonym_init(PG_FUNCTION_ARGS)
{
    List *dictoptions = (List *) PG_GETARG_POINTER(0);
    DictSyn *d;
    ListCell *l;
    char *filename = NULL;
    bool case_sensitive = false;
    tsearch_readline_state trst;
    char *starti, *starto, *end = NULL;
    int cur = 0;
    char *line = NULL;
    uint16 flags = 0;

    // Parse configuration options
    foreach(l, dictoptions)
    {
        DefElem *defel = (DefElem *) lfirst(l);

        if (strcmp(defel->defname, "synonyms") == 0)
            filename = defGetString(defel);
        else if (strcmp(defel->defname, "casesensitive") == 0)
            case_sensitive = defGetBoolean(defel);
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized synonym parameter: \"%s\"",
                           defel->defname)));
    }

    // Synonyms file is required
    if (!filename)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("missing Synonyms parameter")));

    // Open synonyms file
    filename = get_tsearch_config_filename(filename, "syn");
    if (!tsearch_readline_begin(&trst, filename))
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("could not open synonym file \"%s\": %m", filename)));

    d = (DictSyn *) palloc0(sizeof(DictSyn));

    // Read synonym pairs from file
    while ((line = tsearch_readline(&trst)) != NULL)
    {
        // Parse input word
        starti = findwrd(line, &end, NULL);
        if (!starti || *end == '\0')
            goto skipline;  // Skip empty lines or single words
        *end = '\0';

        // Parse output word
        starto = findwrd(end + 1, &end, &flags);
        if (!starto)
            goto skipline;
        *end = '\0';

        // Expand array if needed
        if (cur >= d->len)
        {
            if (d->len == 0)
            {
                d->len = 64;
                d->syn = (Syn *) palloc(sizeof(Syn) * d->len);
            }
            else
            {
                d->len *= 2;
                d->syn = (Syn *) repalloc(d->syn, sizeof(Syn) * d->len);
            }
        }

        // Store synonym pair (with case handling)
        if (case_sensitive)
        {
            d->syn[cur].in = pstrdup(starti);
            d->syn[cur].out = pstrdup(starto);
        }
        else
        {
            d->syn[cur].in = lowerstr(starti);
            d->syn[cur].out = lowerstr(starto);
        }

        d->syn[cur].outlen = strlen(starto);
        d->syn[cur].flags = flags;
        cur++;

skipline:
        pfree(line);
    }

    tsearch_readline_end(&trst);

    // Finalize dictionary: set size, sort for binary search
    d->len = cur;
    qsort(d->syn, d->len, sizeof(Syn), compareSyn);
    d->case_sensitive = case_sensitive;

    PG_RETURN_POINTER(d);
}
```