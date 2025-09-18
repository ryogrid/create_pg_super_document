# mark_fragment

## Location
src/backend/tsearch/wparser_def.c: 2184 - 2219

## Overview
Applies appropriate highlight marking to a range of words selected by the headline selector for PostgreSQL text search result highlighting.

## Definition
```c
static void mark_fragment(HeadlineParsedText *prs, bool highlightall, int startpos, int endpos)
```

## Detailed Description
The `mark_fragment` function is responsible for marking words within a specified range for highlighting in PostgreSQL text search results. It processes each word in the range from startpos to endpos (inclusive) and applies different marking strategies based on the highlightall flag and word type. For non-highlightall mode, it uses HLIDREPLACE and HLIDSKIP macros to determine replacement or skip behavior. For highlightall mode, it uses XMLHLIDSKIP for XML-aware highlighting. The function also handles repeated words by setting the "in" flag appropriately.

## Parameters / Member Variables
- `prs`: HeadlineParsedText structure containing the parsed words array
- `highlightall`: Boolean flag determining highlighting strategy (XML vs standard)
- `startpos`: Starting word index (inclusive) for the fragment to mark
- `endpos`: Ending word index (inclusive) for the fragment to mark

## Dependencies
- Functions called/Symbols referenced:
  - HeadlineParsedText (headline parsed text structure)
  - HLIDREPLACE (macro for checking if word type should be replaced)
  - HLIDSKIP (macro for checking if word type should be skipped)
  - XMLHLIDSKIP (macro for XML-aware skip checking)
- Called from (representative examples):
  - mark_hl_fragments (src/backend/tsearch/wparser_def.c:2414, 2444)
  - mark_hl_words (src/backend/tsearch/wparser_def.c:2609)

## Notes and Other Information
- Sets selected=1 for words that contain query items
- Handles two distinct highlighting modes: standard and highlightall (XML)
- Uses different skip/replace logic based on the highlighting mode
- Manages repeated word handling by setting the "in" flag to 0 for repeated words
- Part of PostgreSQL's text search highlighting pipeline
- Located in src/backend/tsearch/wparser_def.c:2184-2219