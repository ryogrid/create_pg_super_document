# calc_rank_and

## Location
[src/backend/utils/adt/tsrank.c:200-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L200-L282)

## Overview
Calculates text search ranking for AND operations between query terms, considering positional proximity of matching words to determine relevance scores.

## Definition

```c
static float
calc_rank_and(const float *w, TSVector t, TSQuery q)
```
## Detailed Description
This function implements the AND-based ranking algorithm for PostgreSQL's text search functionality. It calculates a relevance score by analyzing the positional proximity of multiple query terms within a document. The algorithm considers the distance between matching terms, with closer terms receiving higher relevance scores. When fewer than 2 unique terms are found, it falls back to OR-based ranking via calc_rank_or.

The ranking computation uses a probabilistic model where the final score represents the likelihood that the document is relevant based on the proximity of query terms. The algorithm iterates through all combinations of term positions and calculates weighted distances, combining individual term scores using the formula: .

## Parameters / Member Variables
-  00:49:11 up 5 days,  4:16,  0 users,  load average: 0.28, 0.36, 0.41
USER     TTY      FROM             LOGIN@   IDLE   JCPU   PCPU WHAT: Array of floating-point weights for different term positions/frequencies
- : TSVector containing the document's lexemes and their positions
- : TSQuery containing the search query terms and operators

## Dependencies
- Functions called/Symbols referenced:
  - [SortAndUniqItems](../S/SortAndUniqItems.md)
  - [calc_rank_or](calc_rank_or.md)
  - [find_wordentry](../f/find_wordentry.md)
  - _POSVECPTR
  - WEP_GETPOS
  - WEP_SETPOS
  - wpos
  - [word_distance](../w/word_distance.md)
- Called from (representative examples):
  - [calc_rank](calc_rank.md)
  - DEF_NORM_METHOD

## Notes and Other Information
- Falls back to OR-based ranking when fewer than 2 unique query terms are present
- Uses a dummy position vector (POSNULL) when positional information is not available
- Implements distance-based weighting where closer terms contribute more to the relevance score
- The algorithm handles both positional and non-positional text search vectors
- Memory management includes proper cleanup of allocated position arrays

## Simplified Source

```c
static float
calc_rank_and(const float *w, TSVector t, TSQuery q)
{
    WordEntryPosVector **pos;
    int i, k, l, p;
    WordEntry *entry, *firstentry;
    WordEntryPos *post, *ct;
    int32 dimt, lenct, dist, nitem;
    float res = -1.0;
    QueryOperand **item;
    int size = q->size;

    // Get sorted unique operands from query
    item = SortAndUniqItems(q, &size);
    if (size < 2)
    {
        pfree(item);
        return calc_rank_or(w, t, q);  // Fall back to OR ranking
    }

    // Allocate position vector array
    pos = (WordEntryPosVector **) palloc0(sizeof(WordEntryPosVector *) * q->size);

    // For each unique operand
    for (i = 0; i < size; i++)
    {
        // Find word entry in document
        firstentry = entry = find_wordentry(t, q, item[i], &nitem);
        if (!entry)
            continue;

        // Process each occurrence of this word
        while (entry - firstentry < nitem)
        {
            // Get position information
            pos[i] = entry->haspos ? _POSVECPTR(t, entry) : POSNULL;

            dimt = pos[i]->npos;
            post = pos[i]->pos;

            // Compare with all previous operands
            for (k = 0; k < i; k++)
            {
                if (!pos[k])
                    continue;

                lenct = pos[k]->npos;
                ct = pos[k]->pos;

                // Calculate proximity scores for all position combinations
                for (l = 0; l < dimt; l++)
                {
                    for (p = 0; p < lenct; p++)
                    {
                        dist = abs((int) WEP_GETPOS(post[l]) - (int) WEP_GETPOS(ct[p]));
                        if (dist || (dist == 0 && (pos[i] == POSNULL || pos[k] == POSNULL)))
                        {
                            float curw;
                            if (!dist)
                                dist = MAXENTRYPOS;

                            // Calculate weighted proximity score
                            curw = sqrt(wpos(post[l]) * wpos(ct[p]) * word_distance(dist));
                            res = (res < 0) ? curw : 1.0 - (1.0 - res) * (1.0 - curw);
                        }
                    }
                }
            }
            entry++;
        }
    }

    pfree(pos);
    pfree(item);
    return res;
}
```

This simplified version shows the AND ranking algorithm: get unique operands, find their positions in the document, then calculate proximity-based scores by comparing distances between all operand pairs. Closer terms get higher scores using weighted distance calculations.