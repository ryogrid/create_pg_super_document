# prsd_headline

## Location
src/backend/tsearch/wparser_def.c: 2616 - 2724

## Overview
The main headline function for PostgreSQL's default text search parser, responsible for generating highlighted text excerpts from documents based on search queries and configuration options.

## Definition


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
  - defGetString (extract string values from configuration options)
  - pg_strtoint32 (parse integer configuration values)
  - TS_execute_locations (execute query against document to find matches)
  - GETQUERY (extract query from TSQuery structure)
  - checkcondition_HL (condition checking function for highlighting)
  - mark_hl_words (single fragment headline selection)
  - mark_hl_fragments (multi-fragment headline selection)
  - pstrdup (duplicate strings in PostgreSQL memory context)
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