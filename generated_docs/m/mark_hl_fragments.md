# mark_hl_fragments

## Location
[src/backend/tsearch/wparser_def.c:2271-2453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L2271-L2453)

## Overview
A headline selector function used specifically when MaxFragments > 0, responsible for selecting and marking the best text fragments for highlighting in PostgreSQL's text search functionality.

## Definition

```c
static void
mark_hl_fragments(HeadlineParsedText *prs, TSQuery query, List *locations,
				  bool highlightall,
				  int shortword, int min_words,
				  int max_words, int max_fragments)
```
## Detailed Description
This function implements a sophisticated fragment selection algorithm for text search highlighting when multiple fragments are requested. It analyzes the parsed text to find query word covers (continuous spans containing query words), breaks them into manageable fragments of at most max_words length, and then selects the best fragments based on query word density and fragment length.

The selection process prioritizes fragments with the maximum number of query words, and in case of ties, chooses fragments with fewer total words. Selected fragments can be stretched within the max_words limit to provide better context while ensuring good start/end points.

If no suitable fragments are found, it falls back to showing the first min_words of the document.

## Parameters / Member Variables
- `*prs`: HeadlineParsedText structure containing the parsed document words and metadata
- `query`: TSQuery object containing the search query terms to highlight
- `*locations`: List of query word locations in the document
- `highlightall`: Boolean flag controlling presentation details (disregarded for phrase selection in this mode)
- `shortword`: Minimum word length threshold (parameter present but not actively used in fragment selection logic)
- `min_words`: Minimum number of words to show if no good fragments are found
- `max_words`: Maximum number of words allowed per fragment
- `max_fragments`: Maximum number of fragments to select and mark
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

## Simplified Source

```c
static void mark_hl_fragments(HeadlineParsedText *prs, TSQuery query, List *locations,
                             bool highlightall, int shortword, int min_words,
                             int max_words, int max_fragments) {
    int32 numcovers = 0, maxcovers = 32;
    int32 startpos, endpos, nextpos, p, q;
    int32 num_f = 0;  // Number of fragments found
    CoverPos *covers = palloc(maxcovers * sizeof(CoverPos));

    // Find all query word covers in the document
    while (hlCover(prs, query, locations, &nextpos, &p, &q)) {
        startpos = p;
        endpos = q;

        // Break large covers into max_words fragments
        while (startpos <= endpos) {
            int32 curlen, poslen;
            get_next_fragment(prs, &startpos, &endpos, &curlen, &poslen, max_words);

            // Expand covers array if needed
            if (numcovers >= maxcovers) {
                maxcovers *= 2;
                covers = repalloc(covers, sizeof(CoverPos) * maxcovers);
            }

            // Store fragment information
            covers[numcovers].startpos = startpos;
            covers[numcovers].endpos = endpos;
            covers[numcovers].curlen = curlen;
            covers[numcovers].poslen = poslen;
            covers[numcovers].chosen = false;
            covers[numcovers].excluded = false;
            numcovers++;

            startpos = endpos + 1;
            endpos = q;
        }
    }

    // Select best fragments (max query words, then min total words)
    for (int f = 0; f < max_fragments; f++) {
        int32 maxitems = 0, minwords = PG_INT32_MAX, minI = -1;

        // Find best remaining cover
        for (int i = 0; i < numcovers; i++) {
            if (!covers[i].chosen && !covers[i].excluded &&
                (maxitems < covers[i].poslen ||
                 (maxitems == covers[i].poslen && minwords > covers[i].curlen))) {
                maxitems = covers[i].poslen;
                minwords = covers[i].curlen;
                minI = i;
            }
        }

        if (minI >= 0) {
            covers[minI].chosen = true;
            startpos = covers[minI].startpos;
            endpos = covers[minI].endpos;
            int32 curlen = covers[minI].curlen;

            // Stretch fragment to max_words if possible
            if (curlen < max_words) {
                int32 maxstretch = (max_words - curlen) / 2;

                // Stretch backward from start
                int stretch = 0, posmarker = startpos;
                for (int i = startpos - 1; i >= 0 && stretch < maxstretch && !prs->words[i].in; i--) {
                    if (!NONWORDTOKEN(prs->words[i].type)) {
                        curlen++;
                        stretch++;
                    }
                    posmarker = i;
                }

                // Find good start boundary
                for (int i = posmarker; i < startpos && BADENDPOINT(i); i++) {
                    if (!NONWORDTOKEN(prs->words[i].type))
                        curlen--;
                }
                startpos = i;

                // Stretch forward from end
                posmarker = endpos;
                for (int i = endpos + 1; i < prs->curwords && curlen < max_words && !prs->words[i].in; i++) {
                    if (!NONWORDTOKEN(prs->words[i].type))
                        curlen++;
                    posmarker = i;
                }

                // Find good end boundary
                for (int i = posmarker; i > endpos && BADENDPOINT(i); i--) {
                    if (!NONWORDTOKEN(prs->words[i].type))
                        curlen--;
                }
                endpos = i;
            }

            // Mark the selected fragment
            mark_fragment(prs, highlightall, startpos, endpos);
            num_f++;

            // Exclude overlapping covers
            for (int i = 0; i < numcovers; i++) {
                if (i != minI &&
                    ((covers[i].startpos >= startpos && covers[i].startpos <= endpos) ||
                     (covers[i].endpos >= startpos && covers[i].endpos <= endpos) ||
                     (covers[i].startpos < startpos && covers[i].endpos > endpos)))
                    covers[i].excluded = true;
            }
        } else {
            break;  // No more selectable covers
        }
    }

    // Fallback: show first min_words if no fragments found
    if (num_f <= 0) {
        int32 curlen = 0;
        endpos = -1;
        for (int i = 0; i < prs->curwords && curlen < min_words; i++) {
            if (!NONWORDTOKEN(prs->words[i].type))
                curlen++;
            endpos = i;
        }
        mark_fragment(prs, highlightall, 0, endpos);
    }

    pfree(covers);
}
```