# mark_hl_words

## Location
[src/backend/tsearch/wparser_def.c:2454-2615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L2454-L2615)

## Overview
A headline selector function used when MaxFragments == 0, responsible for selecting and marking the single best text fragment for highlighting in PostgreSQL's text search functionality.

## Definition

```c
static void
mark_hl_words(HeadlineParsedText *prs, TSQuery query, List *locations,
			  bool highlightall,
			  int shortword, int min_words, int max_words)
```
## Detailed Description
This function implements a headline selection algorithm for single-fragment mode (when MaxFragments is 0). It evaluates all possible query word covers and selects the best one based on a sophisticated scoring system that prioritizes: 1) coverage of the original query span, 2) number of interesting words, and 3) good endpoint quality.

The function can operate in two modes: normal mode where it finds the optimal fragment around query words, and highlightall mode where it marks the entire document. In normal mode, it attempts to expand fragments to meet minimum word requirements while staying within the maximum word limit, and adjusts fragment boundaries to avoid bad endpoints when possible.

## Parameters / Member Variables
- `*prs`: HeadlineParsedText structure containing the parsed document words and metadata
- `query`: TSQuery object containing the search query terms to highlight
- `*locations`: List of query word locations in the document
- `highlightall`: Boolean flag - if true, marks entire document; if false, selects optimal fragment
- `shortword`: Minimum word length threshold (parameter present but not actively used in this function)
- `min_words`: Minimum number of words required in the selected headline fragment
- `max_words`: Maximum number of words allowed in the selected headline fragment
## Dependencies
- Functions called/Symbols referenced:
  - [hlCover](../h/hlCover.md) (finds query word covers/spans)
  - [mark_fragment](mark_fragment.md) (marks the selected fragment for highlighting)
  - NONWORDTOKEN (macro to check if token is a non-word)
  - INTERESTINGWORD (macro to check if word is interesting/relevant)
  - BADENDPOINT (macro to check if position is a bad fragment endpoint)
- Called from:
  - [prsd_headline](../p/prsd_headline.md) (main headline generation function)

## Notes and Other Information
- Uses a multi-criteria selection algorithm with priority ordering: cover inclusion > interesting word count > endpoint quality
- Attempts to expand fragments in both directions when they fall short of min_words requirement
- Can contract fragments that exceed max_words while trying to maintain good endpoints
- Falls back to showing the first min_words of the document if no suitable query-based fragment is found
- The 'cover' concept refers to a continuous span of text that contains query words from the search
- Optimizes for readability by avoiding fragments that end at awkward positions (bad endpoints)

## Simplified Source

```c
static void mark_hl_words(HeadlineParsedText *prs, TSQuery query, List *locations,
                         bool highlightall, int shortword, int min_words, int max_words) {
    int nextpos = 0, p = 0, q = 0;
    int bestb = -1, beste = -1, bestlen = -1;
    bool bestcover = false;
    int pose, posb, poslen, curlen;
    bool poscover;

    if (!highlightall) {
        // Find best cover among all possible covers
        while (hlCover(prs, query, locations, &nextpos, &p, &q)) {
            // Count words and interesting words within cover up to max_words
            curlen = 0;
            poslen = 0;
            posb = pose = p;

            for (int i = p; i <= q && curlen < max_words; i++) {
                if (!NONWORDTOKEN(prs->words[i].type))
                    curlen++;
                if (INTERESTINGWORD(i))
                    poslen++;
                pose = i;
            }

            if (curlen < max_words) {
                // Try to extend headline forward to max_words or good endpoint
                for (int i = i - 1; i < prs->curwords && curlen < max_words; i++) {
                    if (i > q) {
                        if (!NONWORDTOKEN(prs->words[i].type))
                            curlen++;
                        if (INTERESTINGWORD(i))
                            poslen++;
                    }
                    pose = i;
                    if (BADENDPOINT(i))
                        continue;
                    if (curlen >= min_words)
                        break;
                }

                // If still too short, try extending backward
                if (curlen < min_words) {
                    for (int i = p - 1; i >= 0; i--) {
                        if (!NONWORDTOKEN(prs->words[i].type))
                            curlen++;
                        if (INTERESTINGWORD(i))
                            poslen++;
                        if (curlen >= max_words)
                            break;
                        if (BADENDPOINT(i))
                            continue;
                        if (curlen >= min_words)
                            break;
                    }
                    posb = (i >= 0) ? i : 0;
                }
            } else {
                // Fragment too long - try to shorten from end to avoid bad endpoint
                if (i > q)
                    i = q;
                for (; curlen > min_words; i--) {
                    if (!BADENDPOINT(i))
                        break;
                    if (!NONWORDTOKEN(prs->words[i].type))
                        curlen--;
                    if (INTERESTINGWORD(i))
                        poslen--;
                    pose = i - 1;
                }
            }

            // Check if proposed headline includes original cover
            poscover = (posb <= p && pose >= q);

            // Choose best headline: cover inclusion > interesting words > good endpoints
            if (poscover > bestcover ||
                (poscover == bestcover && poslen > bestlen) ||
                (poscover == bestcover && poslen == bestlen &&
                 !BADENDPOINT(pose) && BADENDPOINT(beste))) {
                bestb = posb;
                beste = pose;
                bestlen = poslen;
                bestcover = poscover;
            }
        }

        // Fallback: show first min_words if no acceptable headline found
        if (bestlen < 0) {
            curlen = 0;
            pose = -1;
            for (int i = 0; i < prs->curwords && curlen < min_words; i++) {
                if (!NONWORDTOKEN(prs->words[i].type))
                    curlen++;
                pose = i;
            }
            bestb = 0;
            beste = pose;
        }
    } else {
        // Highlightall mode: show entire document
        bestb = 0;
        beste = prs->curwords - 1;
    }

    mark_fragment(prs, highlightall, bestb, beste);
}
```