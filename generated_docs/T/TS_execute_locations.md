# TS_execute_locations

## Location
src/backend/utils/adt/tsvector_op.c: 2007 - 2024

## Overview
TS_execute_locations evaluates tsquery expressions and returns detailed location information for matching terms, providing both match success/failure status and precise lexeme position data for all matched components.

## Definition


## Detailed Description
This function extends the basic tsquery execution model to provide detailed positional information about where matches occur within the text. Unlike the simpler TS_execute variants that only return boolean or ternary match results, this function captures and returns the specific lexeme positions for all successful matches.

Key operational features:
- Returns a List of ExecPhraseData structs containing position arrays for each matched component
- Handles complex boolean expressions while preserving location information
- Processes OR operations by union'ing match locations into single list elements
- Reports maximum width for OR'ed phrase operators (conservative approach)
- Ignores NOT subexpression locations since they represent negative matches
- Requires position data from callback functions (fails if positions unavailable)

Result structure:
- Each List element corresponds to an AND'ed term or phrase operator
- Each ExecPhraseData struct contains sorted arrays of matching lexeme positions
- For phrase operators, positions represent the rightmost lexeme of width+1 lexeme matches
- OR operations consolidate multiple match locations into unified position sets

The function is particularly important for headline generation and advanced text processing where knowing exactly where matches occur is essential.

## Parameters / Member Variables
- : Pointer to the first QueryItem in the tsquery expression tree
- : Opaque argument passed through to the TSExecuteCallback function
- : Execution control flags (currently only TS_EXEC_EMPTY is supported)
- : Callback function that must provide position data for lexeme matches

## Dependencies
- Functions called/Symbols referenced:
  - TS_execute_locations_recurse
  - TS_EXEC_EMPTY (flag verification)
  - Assert
- Called from (representative examples):
  - prsd_headline (in wparser_def.c)

## Notes and Other Information
- Essential for headline generation functionality in PostgreSQL's text search system
- The conservative approach for OR'ed phrase operators may report slightly wider matches than necessary
- Requires callback functions to provide position information, unlike other TS_execute variants
- Returns NIL for both non-matches and cases where position data is unavailable
- Currently supports no execution flags beyond the empty flag set
- The position reporting follows the rule that phrase matches record the position of the rightmost lexeme
- Critical for applications requiring detailed match location analysis rather than simple boolean results