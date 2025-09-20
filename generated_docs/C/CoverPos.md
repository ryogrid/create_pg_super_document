# CoverPos

## Location
[src/backend/tsearch/wparser_def.c:1963-1969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L1963-L1969)

## Overview
CoverPos is a structure that represents a text fragment (or "cover") used in PostgreSQL's full-text search headline generation to track potential highlighting regions.

## Definition

```c
typedef struct
{
	/* callback data for checkcondition_HL */
	HeadlineWordEntry *words;
	int			len;
} hlCheck;
```
## Detailed Description
CoverPos is a data structure used in PostgreSQL's text search headline generation functionality, specifically within the mark_hl_fragments function. It represents a potential text fragment that could be highlighted in search results. The structure tracks the boundaries and properties of text segments that contain query-matching words, allowing the headline generation algorithm to select the most relevant fragments for display.

The structure supports the algorithm's process of finding optimal text fragments that contain the maximum number of query-matching terms while staying within specified length constraints. Multiple CoverPos structures are typically created and evaluated to determine the best fragments to highlight in the final headline output.

## Parameters / Member Variables
- : The starting word index of the text fragment within the parsed text
- : The ending word index of the fragment (inclusive boundary)
- : The count of "interesting" words (query-matching terms) within this fragment
- : The total number of words contained in this fragment
- : Boolean flag indicating whether this fragment has been selected for highlighting
- : Boolean flag indicating whether this fragment has been excluded from consideration

## Dependencies
- Functions called/Symbols referenced:
  - [HeadlineWordEntry](../H/HeadlineWordEntry.md) (indirectly through related structures)
- Called from (representative examples):
  - [mark_hl_fragments](../m/mark_hl_fragments.md) (primary usage location)

## Notes and Other Information
- Used specifically in headline generation for full-text search results
- Part of PostgreSQL's text search infrastructure for creating snippet previews
- The structure helps optimize fragment selection by tracking both content quality (interesting words) and size constraints
- Arrays of CoverPos structures are dynamically allocated and managed during headline processing
- The chosen and excluded flags help implement selection algorithms that avoid overlapping fragments
- Essential for implementing features like maximum fragment count and minimum/maximum word limits in headlines