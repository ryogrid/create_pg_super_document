# SpGistLastUsedPage

## Location
src/include/access/spgist_private.h: 99 - 103

## Overview
SpGistLastUsedPage is a structure that caches information about the last-used page for efficient space management in SP-GiST indexes.

## Definition
```c
typedef struct SpGistLastUsedPage
{
    BlockNumber blkno;      /* block number, or InvalidBlockNumber */
    int         freeSpace;  /* page's free space (could be obsolete!) */
} SpGistLastUsedPage;
```

## Detailed Description
SpGistLastUsedPage is a caching structure used to optimize page allocation and space management in SP-GiST indexes. Each backend maintains a cache of last-used page information in the index's rd_amcache area. This cache is initialized from and occasionally synchronized with shared storage in the index metapage. The purpose is to quickly identify pages that likely have available free space for new tuple insertions, reducing the need to scan multiple pages to find suitable insertion locations.

## Parameters / Member Variables
- `blkno`: The block number of the cached page, or InvalidBlockNumber if no page is currently cached
- `freeSpace`: The amount of free space available on the cached page (note: this information could become obsolete if other backends modify the page)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - [SpGistGetBuffer](SpGistGetBuffer.md) (in src/backend/access/spgist/spgutils.c)
  - [SpGistSetLastUsedPage](SpGistSetLastUsedPage.md) (in src/backend/access/spgist/spgutils.c)
  - [SpGistLUPCache](SpGistLUPCache.md) (in src/include/access/spgist_private.h)

## Notes and Other Information
- This structure is part of a performance optimization strategy to avoid repeatedly scanning for pages with available space
- The cached free space information may become stale due to concurrent modifications by other backends
- The cache is maintained per-backend and is periodically synchronized with the shared metapage
- InvalidBlockNumber is used as a sentinel value to indicate that no page is currently cached
- This caching mechanism helps reduce I/O operations during index insertions by providing hints about where space might be available