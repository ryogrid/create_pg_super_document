# hlCover

## Location
src/backend/tsearch/wparser_def.c: 2032 - 2183

## Overview
Finds the minimal substring of parsed headline words that satisfies a given TSQuery, using lexeme position information to optimize text search highlighting.

## Definition
```c
static bool hlCover(HeadlineParsedText *prs, TSQuery query, List *locations, int *nextpos, int *p, int *q)
```

## Detailed Description
The `hlCover` function is a core component of PostgreSQL's text search highlighting system. It attempts to find a minimal cover—the shortest possible substring of words that satisfies all conditions of a given TSQuery. The function works by analyzing lexeme positions from TS_execute_locations() results to identify plausible query-matching subranges. It employs a two-phase approach: first finding the earliest positions where all AND-ed query terms occur, then finding the latest positions where they can start, creating a minimal bounding range. The function ensures the result is truly minimal by requiring both endpoints to be actual query-matching words.

## Parameters / Member Variables
- `prs`: HeadlineParsedText structure containing the parsed words array
- `query`: TSQuery object representing the search query to satisfy
- `locations`: List of ExecPhraseData containing pre-computed lexeme positions
- `nextpos`: Pointer to starting lexeme position for search (updated on success)
- `p`: Pointer to store first word index of the cover substring
- `q`: Pointer to store last word index of the cover substring

## Dependencies
- Functions called/Symbols referenced:
  - HeadlineParsedText (headline parsed text structure)
  - TSQuery (text search query type)
  - ExecPhraseData (execution phrase data from TS_execute_locations)
  - TS_execute (executes query against word subset)
  - GETQUERY (extracts query from TSQuery)
  - checkcondition_HL (callback for condition checking)
  - TS_EXEC_EMPTY (execution flag for empty handling)
  - hlCheck (headline check structure)
- Called from (representative examples):
  - mark_hl_fragments (src/backend/tsearch/wparser_def.c:2302)
  - mark_hl_words (src/backend/tsearch/wparser_def.c:2475)

## Notes and Other Information
- Returns true on successful match, false when no valid cover can be found
- Produces minimal covers where both endpoints are actual query-matching words
- Handles complex query structures including phrases, AND/OR combinations, and NOT conditions
- Uses lexeme positions rather than word indexes to maintain phrase matching accuracy
- Includes fallback logic for edge cases involving phrase matches OR-ed with plain terms
- Part of PostgreSQL's advanced text search highlighting system
- Located in src/backend/tsearch/wparser_def.c:2032-2183