# PageInit

## Location
[src/backend/storage/page/bufpage.c:42-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L42-L87)

## Overview
Initializes the contents of a page in PostgreSQL's buffer pool, setting up the basic page structure and metadata without calculating an initial checksum.

## Definition

```c
void
PageInit(Page page, Size pageSize, Size specialSize)
```
## Detailed Description
PageInit is a fundamental function that initializes a PostgreSQL page structure. It sets up the page header fields and establishes the initial layout for data storage. The function zeroes out the entire page content, then configures the page header with proper values for page management. It sets the lower boundary to just after the header and the upper boundary to account for any special space at the end of the page. The function ensures proper alignment of the special space and validates that the page size matches the expected block size.

## Parameters / Member Variables
- `page`: Pointer to the page buffer to be initialized
- `pageSize`: Size of the page (must equal BLCKSZ)
- `specialSize`: Size of special space to reserve at the end of the page
## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - MAXALIGN (alignment macro)
  - MemSet (memory clearing function)
  - [PageSetPageSizeAndVersion](PageSetPageSizeAndVersion.md) (page version setting function)
  - SizeOfPageHeaderData (constant for header size)
  - PG_PAGE_LAYOUT_VERSION (page layout version constant)
- Called from (representative examples):
  - [brin_page_init](../b/brin_page_init.md) (BRIN index page initialization)
  - [GinInitPage](../G/GinInitPage.md) (GIN index page initialization)
  - [gistinitpage](../g/gistinitpage.md) (GiST index page initialization)
  - [_hash_pageinit](../h/_hash_pageinit.md) (hash index page initialization)
  - [_bt_pageinit](../b/_bt_pageinit.md) (B-tree index page initialization)

## Notes and Other Information
- The function does not calculate an initial checksum - this is deferred until write time
- All page fields are zeroed before setting specific values
- The special space is aligned to MAXALIGN boundaries
- Page size validation ensures it matches BLCKSZ and provides sufficient space for header and special area
- The pd_prune_xid field is left as InvalidTransactionId through the MemSet operation

## Simplified Source
```c
void PageInit(Page page, Size pageSize, Size specialSize) {
    PageHeader p = (PageHeader) page;

    // Align special space to proper boundary
    specialSize = MAXALIGN(specialSize);

    // Validate page size and space requirements
    Assert(pageSize == BLCKSZ);
    Assert(pageSize > specialSize + SizeOfPageHeaderData);

    // Zero out entire page
    MemSet(p, 0, pageSize);

    // Initialize page header fields
    p->pd_flags = 0;
    p->pd_lower = SizeOfPageHeaderData;           // End of header
    p->pd_upper = pageSize - specialSize;        // Start of special space
    p->pd_special = pageSize - specialSize;      // Special space location

    // Set page size and layout version
    PageSetPageSizeAndVersion(page, pageSize, PG_PAGE_LAYOUT_VERSION);
}
```