# get_next_fragment

## Location
[src/backend/tsearch/wparser_def.c:2220-2270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L2220-L2270)

## Overview
Splits a cover substring into fragments not longer than max_words, ensuring fragments start and end with query-matching words for optimal headline display.

## Definition
```c
static void get_next_fragment(HeadlineParsedText *prs, int *startpos, int *endpos, int *curlen, int *poslen, int max_words)
```

## Detailed Description
The `get_next_fragment` function is designed to intelligently split text search cover substrings into manageable fragments for headline display. It takes a cover substring bounded by startpos and endpos and creates a fragment that respects the max_words limit while ensuring both endpoints contain interesting (query-matching) words. The function operates in three phases: first moving startpos to an interesting word, then counting words up to max_words limit, and finally adjusting endpos backward to an interesting word if the fragment was truncated. This ensures that displayed fragments are both size-appropriate and contextually meaningful.

## Parameters / Member Variables
- `prs`: HeadlineParsedText structure containing the parsed words array
- `startpos`: Pointer to starting position of the cover substring (updated to fragment start)
- `endpos`: Pointer to ending position of the cover substring (updated to fragment end)  
- `curlen`: Pointer to store the fragment length in total words
- `poslen`: Pointer to store the fragment length in interesting words only
- `max_words`: Maximum number of words allowed in the fragment

## Dependencies
- Functions called/Symbols referenced:
  - [HeadlineParsedText](../H/HeadlineParsedText.md) (headline parsed text structure)
  - INTERESTINGWORD (macro to check if a word index contains query-matching content)
  - NONWORDTOKEN (macro to check if a token type represents non-word content)
- Called from (representative examples):
  - [mark_hl_fragments](../m/mark_hl_fragments.md) (src/backend/tsearch/wparser_def.c:2316)

## Notes and Other Information
- Ensures fragments start and end with interesting (query-matching) words for better context
- Handles both total word count (curlen) and interesting word count (poslen) tracking
- Automatically returns the entire cover if it contains fewer than max_words
- Uses backward adjustment to preserve interesting words when truncating fragments
- Part of PostgreSQL's advanced text search highlighting and fragment selection system
- Located in src/backend/tsearch/wparser_def.c:2220-2270