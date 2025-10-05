# prsd_headline

## Location
[src/backend/tsearch/wparser_def.c:2616-2724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L2616-L2724)

## Overview
The main headline function for PostgreSQL's default text search parser, responsible for generating highlighted text excerpts from documents based on search queries and configuration options.

## Definition

```c
Datum
prsd_headline(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the entry point for headline generation in PostgreSQL's full-text search system. It processes configuration options, validates parameters, locates query matches within the parsed text, and delegates to appropriate headline selection algorithms based on the MaxFragments setting.

The function supports two main modes: single fragment mode (MaxFragments = 0) which selects the best single excerpt, and multi-fragment mode (MaxFragments > 0) which can select multiple text excerpts. It handles various configuration parameters including word limits, highlighting markers, fragment delimiters, and special modes like HighlightAll.

Key responsibilities include: parsing and validating headline options, executing the query against the document to find matching locations, choosing the appropriate headline selection strategy, and setting up default highlighting markup if not specified.

## Parameters / Member Variables
Function uses PostgreSQL's PG_FUNCTION_ARGS mechanism with three arguments:
- : HeadlineParsedText structure containing the parsed document and results
- : List of configuration options for headline generation
- : TSQuery object containing the search terms

Configuration options processed:
- : Maximum words per fragment (default: 35)
- : Minimum words per fragment (default: 15)  
- : Minimum word length threshold (default: 3)
- : Number of fragments to generate (default: 0 = single fragment)
- : Opening highlight tag (default: "<b>")
- : Closing highlight tag (default: "</b>")
- : Separator between fragments (default: " ... ")
- : Boolean to highlight entire document (default: false)

## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](../d/defGetString.md) (extract string values from configuration options)
  - [pg_strtoint32](pg_strtoint32.md) (parse integer configuration values)
  - [TS_execute_locations](../T/TS_execute_locations.md) (execute query against document to find matches)
  - GETQUERY (extract query from TSQuery structure)
  - [checkcondition_HL](../c/checkcondition_HL.md) (condition checking function for highlighting)
  - [mark_hl_words](../m/mark_hl_words.md) (single fragment headline selection)
  - [mark_hl_fragments](../m/mark_hl_fragments.md) (multi-fragment headline selection)
  - [pstrdup](pstrdup.md) (duplicate strings in PostgreSQL memory context)
- Called from:
  - Used as a PostgreSQL function callable from SQL queries via the text search system

## Notes and Other Information
- Implements parameter validation with appropriate error reporting for invalid configurations
- Uses PostgreSQL's memory management system (palloc/pstrdup) for string allocation
- Supports flexible configuration through key-value options list
- Handles empty queries gracefully by treating them as no matches
- Sets up default HTML-style highlighting markers if none are specified
- The function follows PostgreSQL's fmgr (function manager) calling convention
- HighlightAll mode bypasses most parameter validation since fragment limits don't apply
- Returns a pointer to the modified HeadlineParsedText structure for further processing

## Simplified Source

```c
Datum
prsd_headline(PG_FUNCTION_ARGS)
{
    HeadlineParsedText *prs = (HeadlineParsedText *) PG_GETARG_POINTER(0);
    List *prsoptions = (List *) PG_GETARG_POINTER(1);
    TSQuery query = PG_GETARG_TSQUERY(2);

    // Default configuration values
    int min_words = 15;
    int max_words = 35;
    int shortword = 3;
    int max_fragments = 0;
    bool highlightall = false;

    // Initialize selection markers
    prs->startsel = NULL;
    prs->stopsel = NULL;
    prs->fragdelim = NULL;

    // Process configuration options
    foreach(l, prsoptions) {
        DefElem *defel = (DefElem *) lfirst(l);
        char *val = defGetString(defel);

        if (pg_strcasecmp(defel->defname, "MaxWords") == 0)
            max_words = pg_strtoint32(val);
        else if (pg_strcasecmp(defel->defname, "MinWords") == 0)
            min_words = pg_strtoint32(val);
        else if (pg_strcasecmp(defel->defname, "ShortWord") == 0)
            shortword = pg_strtoint32(val);
        else if (pg_strcasecmp(defel->defname, "MaxFragments") == 0)
            max_fragments = pg_strtoint32(val);
        else if (pg_strcasecmp(defel->defname, "StartSel") == 0)
            prs->startsel = pstrdup(val);
        else if (pg_strcasecmp(defel->defname, "StopSel") == 0)
            prs->stopsel = pstrdup(val);
        else if (pg_strcasecmp(defel->defname, "FragmentDelimiter") == 0)
            prs->fragdelim = pstrdup(val);
        else if (pg_strcasecmp(defel->defname, "HighlightAll") == 0)
            highlightall = /* parse boolean value */;
        // Error for unrecognized parameters
    }

    // Validate parameters (unless in HighlightAll mode)
    if (!highlightall) {
        // Check min_words < max_words, min_words > 0, etc.
    }

    // Find query matches in the document
    List *locations;
    if (query->size > 0) {
        hlCheck ch;
        ch.words = prs->words;
        ch.len = prs->curwords;
        locations = TS_execute_locations(GETQUERY(query), &ch, TS_EXEC_EMPTY, checkcondition_HL);
    } else {
        locations = NIL; // empty query
    }

    // Apply headline selection strategy
    if (max_fragments == 0)
        mark_hl_words(prs, query, locations, highlightall, shortword, min_words, max_words);
    else
        mark_hl_fragments(prs, query, locations, highlightall, shortword, min_words, max_words, max_fragments);

    // Set default highlight markers if not specified
    if (!prs->startsel)
        prs->startsel = pstrdup("<b>");
    if (!prs->stopsel)
        prs->stopsel = pstrdup("</b>");
    if (!prs->fragdelim)
        prs->fragdelim = pstrdup(" ... ");

    // Calculate marker lengths for caller
    prs->startsellen = strlen(prs->startsel);
    prs->stopsellen = strlen(prs->stopsel);
    prs->fragdelimlen = strlen(prs->fragdelim);

    PG_RETURN_POINTER(prs);
}
```