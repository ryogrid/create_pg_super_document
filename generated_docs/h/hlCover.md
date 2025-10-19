# hlCover

## Location
[src/backend/tsearch/wparser_def.c:2032-2183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L2032-L2183)

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
  - [HeadlineParsedText](../H/HeadlineParsedText.md) (headline parsed text structure)
  - TSQuery (text search query type)
  - [ExecPhraseData](../E/ExecPhraseData.md) (execution phrase data from TS_execute_locations)
  - [TS_execute](../T/TS_execute.md) (executes query against word subset)
  - GETQUERY (extracts query from TSQuery)
  - [checkcondition_HL](../c/checkcondition_HL.md) (callback for condition checking)
  - TS_EXEC_EMPTY (execution flag for empty handling)
  - hlCheck (headline check structure)
- Called from (representative examples):
  - [mark_hl_fragments](../m/mark_hl_fragments.md) (src/backend/tsearch/wparser_def.c:2302)
  - [mark_hl_words](../m/mark_hl_words.md) (src/backend/tsearch/wparser_def.c:2475)

## Notes and Other Information
- Returns true on successful match, false when no valid cover can be found
- Produces minimal covers where both endpoints are actual query-matching words
- Handles complex query structures including phrases, AND/OR combinations, and NOT conditions
- Uses lexeme positions rather than word indexes to maintain phrase matching accuracy
- Includes fallback logic for edge cases involving phrase matches OR-ed with plain terms
- Part of PostgreSQL's advanced text search highlighting system
- Located in src/backend/tsearch/wparser_def.c:2032-2183

## Simplified Source

```c
static bool
hlCover(HeadlineParsedText *prs, TSQuery query, List *locations,
        int *nextpos, int *p, int *q)
{
    int pos = *nextpos;

    // Main search loop
    for (;;) {
        int posb, pose;

        // Find latest position where all query terms first appear at/after pos
        pose = -1;
        foreach(lc, locations) {
            ExecPhraseData *pdata = (ExecPhraseData *) lfirst(lc);
            int first = -1;

            // Find first occurrence of this term at/after pos
            for (int i = 0; i < pdata->npos; i++) {
                int endp = pdata->pos[i];
                if (endp >= pos) {
                    first = endp;
                    break;
                }
            }
            if (first < 0)
                return false;  // No more matches for this term
            if (first > pose)
                pose = first;
        }

        if (pose < 0)
            return false;

        // Find earliest position where all terms can start at/before pose
        posb = INT_MAX - 1;
        foreach(lc, locations) {
            ExecPhraseData *pdata = (ExecPhraseData *) lfirst(lc);
            int last = -1;

            // Find last occurrence that can start at/before pose
            for (int i = pdata->npos - 1; i >= 0; i--) {
                int startp = pdata->pos[i] - pdata->width;
                if (startp <= pose) {
                    last = startp;
                    break;
                }
            }
            if (last < posb)
                posb = last;
        }

        posb = Max(posb, pos);

        if (posb <= pose) {
            // Convert lexeme positions to word array indexes
            int idxb = -1, idxe = -1;
            for (int i = 0; i < prs->curwords; i++) {
                if (prs->words[i].item == NULL)
                    continue;
                if (idxb < 0 && prs->words[i].pos >= posb)
                    idxb = i;
                if (prs->words[i].pos <= pose)
                    idxe = i;
                else
                    break;
            }

            if (idxb >= 0 && idxe >= idxb) {
                // Verify the range satisfies the query
                hlCheck ch;
                ch.words = &(prs->words[idxb]);
                ch.len = idxe - idxb + 1;
                if (TS_execute(GETQUERY(query), &ch, TS_EXEC_EMPTY, checkcondition_HL)) {
                    // Found valid cover - update output parameters
                    *nextpos = posb + 1;
                    *p = idxb;
                    *q = idxe;
                    return true;
                }
            }
        }

        // Try next position
        pos = posb + 1;
    }
    return false;
}
```

This simplified version shows the essential algorithm: find the minimal range of words that contains all query terms by calculating the latest first-occurrence positions and earliest last-occurrence positions, convert to word indexes, and verify the range satisfies the query.