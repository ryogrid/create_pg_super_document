# SpGistPageOpaqueData

## Location
[src/include/access/spgist_private.h:60-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist_private.h#L60-L67)

## Overview
SpGistPageOpaqueData is a structure that defines the contents of the page special space on SP-GiST index pages, containing metadata about the page's contents and state.

## Definition
```c
typedef struct SpGistPageOpaqueData
{
    uint16      flags;          /* see bit definitions below */
    uint16      nRedirection;   /* number of redirection tuples on page */
    uint16      nPlaceholder;   /* number of placeholder tuples on page */
    /* note there's no count of either LIVE or DEAD tuples ... */
    uint16      spgist_page_id; /* for identification of SP-GiST indexes */
} SpGistPageOpaqueData;
```

## Detailed Description
SpGistPageOpaqueData represents the opaque data structure stored in the special space of each SP-GiST index page. This structure provides essential metadata about the page's contents and characteristics. The special space is a reserved area at the end of each page used by access methods to store page-specific information. For SP-GiST indexes, this includes flags indicating the page type and state, counts of specific tuple types, and an identifier to verify that the page belongs to an SP-GiST index.

## Parameters / Member Variables
- `flags`: Bit flags indicating various properties of the page (specific flag definitions are referenced elsewhere in the code)
- `nRedirection`: Count of redirection tuples present on this page, used for handling page splits and updates
- `nPlaceholder`: Count of placeholder tuples on this page, which are used during certain index operations
- `spgist_page_id`: A unique identifier to verify that this page belongs to an SP-GiST index structure

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - [SpGistInitPage](SpGistInitPage.md) (in src/backend/access/spgist/spgutils.c)
  - SpGistPageOpaque (in src/include/access/spgist_private.h)
  - SPGIST_PAGE_CAPACITY (in src/include/access/spgist_private.h)

## Notes and Other Information
- This structure is stored in the special space of every SP-GiST index page
- The absence of live and dead tuple counts is noted in the comments, suggesting these are tracked differently
- Redirection and placeholder tuples are special tuple types used in SP-GiST's page management strategy
- The page ID serves as a sanity check to ensure page consistency and proper index identification