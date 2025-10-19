# calc_rank_or

## Location
[src/backend/utils/adt/tsrank.c:283-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L283-L356)

## Overview
Calculates text search ranking for OR operations between query terms, computing relevance scores based on individual term weights and occurrence positions.

## Definition
```c
static float calc_rank_or(const float *w, TSVector t, TSQuery q)
```

## Detailed Description
This function implements the OR-based ranking algorithm for PostgreSQL's text search functionality. It calculates a relevance score by analyzing each query term independently and combining their individual contributions. The algorithm uses a mathematical approach based on the convergent series sum(1/i^2) = π²/6 ≈ 1.64493406685.

For each matching term, the function computes a weighted score considering both the term's weight and its positional information. The scoring formula applies decreasing weights to multiple occurrences of the same term: resj = sum(wi/i^2) where wi represents the weight of the i-th occurrence. The final result is normalized by dividing by the number of unique query terms.

The algorithm includes a noted optimization where instead of sorting weights in descending order (as mathematically ideal), it simply uses the maximum weight for efficiency, as indicated by the TODO comment from Oleg Bartunov.

## Parameters / Member Variables
- `w`: Array of floating-point weights for different term positions/frequencies
- `t`: TSVector containing the document's lexemes and their positions
- `q`: TSQuery containing the search query terms and operators

## Dependencies
- Functions called/Symbols referenced:
  - [SortAndUniqItems](../S/SortAndUniqItems.md)
  - [find_wordentry](../f/find_wordentry.md)
  - POSDATALEN
  - POSDATAPTR
  - wpos
- Called from (representative examples):
  - [calc_rank](calc_rank.md)
  - [calc_rank_and](calc_rank_and.md)
  - DEF_NORM_METHOD

## Notes and Other Information
- Uses the mathematical constant π²/6 ≈ 1.64493406685 for score normalization
- Contains a known optimization where maximum weight is used instead of proper sorting
- Handles both positional and non-positional text search vectors using dummy positions when needed
- Final score is averaged across all unique query terms
- More efficient than AND-based ranking as it doesn't require proximity calculations

## Simplified Source

```c
static float
calc_rank_or(const float *w, TSVector t, TSQuery q)
{
    WordEntry *entry, *firstentry;
    WordEntryPosVector1 posnull;
    WordEntryPos *post;
    int32 dimt, j, i, nitem;
    float res = 0.0;
    QueryOperand **item;
    int size = q->size;

    // Setup dummy position for words without position info
    posnull.npos = 1;
    posnull.pos[0] = 0;

    // Get sorted unique operands
    item = SortAndUniqItems(q, &size);

    // Process each unique operand independently
    for (i = 0; i < size; i++)
    {
        float resj, wjm;
        int32 jm;

        // Find word entry in document
        firstentry = entry = find_wordentry(t, q, item[i], &nitem);
        if (!entry)
            continue;

        // Process each occurrence of this word
        while (entry - firstentry < nitem)
        {
            // Get position information (real or dummy)
            if (entry->haspos)
            {
                dimt = POSDATALEN(t, entry);
                post = POSDATAPTR(t, entry);
            }
            else
            {
                dimt = posnull.npos;
                post = posnull.pos;
            }

            // Calculate score for this word occurrence
            resj = 0.0;
            wjm = -1.0;
            jm = 0;
            for (j = 0; j < dimt; j++)
            {
                // Weight positions with decreasing importance (1/i^2)
                resj = resj + wpos(post[j]) / ((j + 1) * (j + 1));
                if (wpos(post[j]) > wjm)
                {
                    wjm = wpos(post[j]);
                    jm = j;
                }
            }

            // Add to total score using mathematical formula
            // Uses π²/6 ≈ 1.64493406685 for normalization
            res = res + (wjm + resj - wjm / ((jm + 1) * (jm + 1))) / 1.64493406685;

            entry++;
        }
    }

    // Average across all operands
    if (size > 0)
        res = res / size;

    pfree(item);
    return res;
}
```

This simplified version shows the OR ranking algorithm: process each operand independently, calculate weighted scores based on position importance (using 1/i² series), and average across all operands. Uses mathematical constant π²/6 for normalization.