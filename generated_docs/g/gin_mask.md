# gin_mask

## Location
[src/backend/access/gin/ginxlog.c:793-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L793-L813)

## Overview
A specialized page masking function for GIN (Generalized Inverted Index) pages that prepares them for consistency checks by masking variable data elements that should not be compared during verification.

## Definition

```c
void
gin_mask(char *pagedata, BlockNumber blkno)
```
## Detailed Description
The  function is part of PostgreSQL's Write-Ahead Logging (WAL) consistency checking mechanism specifically designed for GIN index pages. It masks (zeros out or modifies) portions of a GIN page that contain variable data that legitimately differs between the primary and standby servers during consistency verification.

The function handles two main scenarios:
1. **Deleted pages**: For pages marked with the GIN_DELETED flag, the entire page content is masked since these pages are initialized to empty
2. **Active pages**: For normal pages, it masks unused space (the "hole" between pd_lower and pd_upper) and other variable elements like LSN, checksum, and hint bits

This masking ensures that consistency checks focus on the actual data content rather than metadata that naturally varies between servers.

## Parameters / Member Variables
- `*pagedata`: Character pointer to the raw page data that needs to be masked
- `blkno`: Block number of the page being processed (currently unused in the implementation but provided for potential future use)
## Dependencies
- Functions called/Symbols referenced:
  - : Masks LSN and checksum fields
  - : Retrieves GIN-specific page metadata
  - : Masks hint bits on the page
  - : Masks entire page content for deleted pages
  - : Masks the unused space hole in active pages
- Types used:
  - : Generic page type
  - : Page header structure
  - : GIN-specific page metadata structure
  - : Block number type
- Constants used:
  - : Flag indicating a deleted GIN page
  - : Size of the standard page header

## Notes and Other Information
- This function is part of the WAL consistency checking infrastructure introduced to verify that standby servers maintain identical data to primary servers
- The function appears to have no direct callers in the current codebase, suggesting it may be called through function pointers or as part of a callback mechanism in the consistency checking framework
- The  parameter is accepted but not used in the current implementation, indicating potential for future enhancements
- GIN deleted pages are handled specially by masking all content since they are always initialized to empty regardless of their previous state
- The function assumes that  has been set correctly when deciding whether to mask unused space

## Simplified Source
```c
void gin_mask(char *pagedata, BlockNumber blkno) {
    Page page = (Page) pagedata;
    PageHeader pagehdr = (PageHeader) page;
    GinPageOpaque opaque;

    // Mask standard page elements (LSN, checksum, hint bits)
    mask_page_lsn_and_checksum(page);
    opaque = GinPageGetOpaque(page);
    mask_page_hint_bits(page);

    // Handle different page types
    if (opaque->flags & GIN_DELETED) {
        // Deleted pages: mask entire content
        mask_page_content(page);
    } else if (pagehdr->pd_lower > SizeOfPageHeaderData) {
        // Active pages: mask unused space only
        mask_unused_space(page);
    }
}
```