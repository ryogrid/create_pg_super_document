# SpGistMetaPageData

## Location
src/include/access/spgist_private.h: 116 - 120

## Overview
SpGistMetaPageData is the structure that defines the contents of the metapage for SP-GiST indexes, containing global metadata and shared cache information.

## Definition
```c
typedef struct SpGistMetaPageData
{
    uint32          magicNumber;     /* for identity cross-check */
    SpGistLUPCache  lastUsedPages;   /* shared storage of last-used info */
} SpGistMetaPageData;
```

## Detailed Description
SpGistMetaPageData represents the structure stored in the metapage of SP-GiST indexes. The metapage is a special page (typically page 0) that contains global information about the index. This structure serves two primary purposes: it provides an identity verification mechanism through a magic number, and it maintains shared storage for the last-used page cache that can be accessed by all backends working with the index. The metapage acts as a coordination point for sharing cache information between different database backends.

## Parameters / Member Variables
- `magicNumber`: A unique identifier used for identity cross-checking to verify that the page is indeed a valid SP-GiST metapage
- `lastUsedPages`: An SpGistLUPCache structure providing shared storage for last-used page information that can be synchronized between different backends

## Dependencies
- Functions called/Symbols referenced:
  - SpGistLUPCache: The cache structure for maintaining last-used page information
- Called from (representative examples):
  - spgGetCache (in src/backend/access/spgist/spgutils.c)
  - SpGistUpdateMetaPage (in src/backend/access/spgist/spgutils.c)
  - SpGistInitMetapage (in src/backend/access/spgist/spgutils.c)
  - SpGistPageGetMeta (in src/include/access/spgist_private.h)

## Notes and Other Information
- The metapage is crucial for SP-GiST index integrity and serves as the coordination point for global index information
- The magic number provides a safety mechanism to detect corruption or misidentification of pages
- The shared last-used page cache helps coordinate efficient page allocation across multiple backends
- This structure is typically stored on the first page (page 0) of the SP-GiST index
- Updates to the metapage require careful synchronization since it's shared among all backends accessing the index