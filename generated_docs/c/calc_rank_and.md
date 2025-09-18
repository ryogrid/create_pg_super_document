# calc_rank_and

## Location
[src/backend/utils/adt/tsrank.c:200-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L200-L282)

## Overview
Calculates text search ranking for AND operations between query terms, considering positional proximity of matching words to determine relevance scores.

## Definition


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