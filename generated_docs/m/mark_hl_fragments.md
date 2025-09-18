# mark_hl_fragments

## Location
[src/backend/tsearch/wparser_def.c:2271-2453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L2271-L2453)

## Overview
A headline selector function used specifically when MaxFragments > 0, responsible for selecting and marking the best text fragments for highlighting in PostgreSQL's text search functionality.

## Definition


## Detailed Description
This function implements a sophisticated fragment selection algorithm for text search highlighting when multiple fragments are requested. It analyzes the parsed text to find query word covers (continuous spans containing query words), breaks them into manageable fragments of at most max_words length, and then selects the best fragments based on query word density and fragment length.

The selection process prioritizes fragments with the maximum number of query words, and in case of ties, chooses fragments with fewer total words. Selected fragments can be stretched within the max_words limit to provide better context while ensuring good start/end points.

If no suitable fragments are found, it falls back to showing the first min_words of the document.

## Parameters / Member Variables
- : HeadlineParsedText structure containing the parsed document words and metadata
- : TSQuery object containing the search query terms to highlight
- : List of query word locations in the document
- : Boolean flag controlling presentation details (disregarded for phrase selection in this mode)
- : Minimum word length threshold (parameter present but not actively used in fragment selection logic)
- : Minimum number of words to show if no good fragments are found
- : Maximum number of words allowed per fragment
- : Maximum number of fragments to select and mark

## Dependencies
- Functions called/Symbols referenced:
  - [hlCover](../h/hlCover.md) (finds query word covers)
  - [get_next_fragment](../g/get_next_fragment.md) (breaks covers into smaller fragments)
  - [mark_fragment](mark_fragment.md) (marks selected fragments for highlighting)
  - [repalloc](../r/repalloc.md) (reallocates memory for cover array)
  - NONWORDTOKEN (macro to check if token is a non-word)
  - BADENDPOINT (macro to check if position is a bad fragment endpoint)
  - PG_INT32_MAX (maximum 32-bit integer constant)
- Called from:
  - [prsd_headline](../p/prsd_headline.md) (main headline generation function)

## Notes and Other Information
- Uses a CoverPos structure array to track potential fragments with their positions, lengths, and selection status
- Implements a greedy selection algorithm that avoids overlapping fragments by excluding them after each selection
- The stretching mechanism attempts to provide better context by expanding fragments up to max_words limit while maintaining good start/end boundaries
- Memory management includes dynamic reallocation of the covers array when needed
- Falls back to showing document beginning if no suitable query-based fragments can be selected