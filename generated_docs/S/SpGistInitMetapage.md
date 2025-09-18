# SpGistInitMetapage

## Location
src/backend/access/spgist/spgutils.c: 724 - 750

## Overview
Initializes the metadata page for an SP-GiST index, setting up the essential metadata structure and cache initialization required for index operations.

## Definition
```c
void SpGistInitMetapage(Page page)
```

## Detailed Description
This function initializes the special metadata page (page 0) of an SP-GiST index. The metadata page contains critical information about the index including a magic number for validation and a cache of recently used pages to optimize insertion performance. The function first calls SpGistInitPage to set up the basic page structure with SPGIST_META flags, then initializes the metadata-specific content.

The function sets up the lastUsedPages cache by initializing all cached page entries to InvalidBlockNumber, indicating they are empty. It also properly sets the page's pd_lower field to ensure that the metadata content is preserved during WAL compression operations. This is crucial for crash recovery and replication.

## Parameters / Member Variables
- `page`: The raw page to be initialized as the SP-GiST metadata page

## Dependencies
- Functions called/Symbols referenced:
  - [SpGistInitPage](SpGistInitPage.md)
  - SpGistPageGetMeta
  - [SpGistMetaPageData](SpGistMetaPageData.md) (struct)
  - SPGIST_META (constant)
  - SPGIST_MAGIC_NUMBER (constant)
  - SPGIST_CACHED_PAGES (constant)
  - InvalidBlockNumber (constant)
  - PageHeader (struct)
- Called from (representative examples):
  - [spgbuild](../s/spgbuild.md)
  - [spgbuildempty](../s/spgbuildempty.md)

## Notes and Other Information
- The metadata page is always the first page (page 0) in an SP-GiST index
- The magic number (SPGIST_MAGIC_NUMBER) provides a way to validate that a page belongs to an SP-GiST index
- The lastUsedPages cache improves insertion performance by tracking pages with available free space
- Setting pd_lower correctly is essential to prevent metadata loss during WAL compression
- This function is called during index creation and when building empty indexes
- The SPGIST_CACHED_PAGES constant determines how many recently used pages are tracked in the cache