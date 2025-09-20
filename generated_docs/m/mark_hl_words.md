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
- : HeadlineParsedText structure containing the parsed document words and metadata
- : TSQuery object containing the search query terms to highlight
- : List of query word locations in the document
- : Boolean flag - if true, marks entire document; if false, selects optimal fragment
- : Minimum word length threshold (parameter present but not actively used in this function)
- : Minimum number of words required in the selected headline fragment
- : Maximum number of words allowed in the selected headline fragment

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