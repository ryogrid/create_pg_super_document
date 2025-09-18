# mask_page_content

## Location
[src/backend/access/common/bufmask.c:119-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/bufmask.c#L119-L130)

## Overview
Masks almost all content of a page, used for index access methods where the contents of deleted pages need to be completely ignored during consistency checks.

## Definition
```c
void mask_page_content(Page page)
```

## Detailed Description
This function provides the most comprehensive page masking by obliterating nearly all page content except for the essential page header fields. It is used for index access methods where deleted or recycled pages may contain arbitrary remnant data that should not be considered during WAL consistency verification.

The function masks two main areas:
1. All page content after the page header (from SizeOfPageHeaderData to the end of the page)
2. The pd_lower and pd_upper fields in the page header, which track free space boundaries

This aggressive masking ensures that pages undergoing deletion or recycling processes don't cause false consistency check failures due to unpredictable remnant content.

## Parameters / Member Variables
- `page`: A pointer to the page whose content should be comprehensively masked

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfPageHeaderData (size of the basic page header structure)
  - MASK_MARKER (constant used to fill masked regions)
  - BLCKSZ (total page/block size constant)
  - PageHeader (type cast for page header access)
  - memset (memory filling function)
- Called from (representative examples):
  - [gin_mask](../g/gin_mask.md) (GIN index masking for deleted pages)
  - [hash_mask](../h/hash_mask.md) (hash index masking for deleted pages)

## Notes and Other Information
- This is the most aggressive masking function in the bufmask suite
- Used specifically for pages that are being deleted or have been deleted in index structures
- Masks everything except critical page header fields like LSN and checksum (which would be handled by other mask functions)
- The pd_lower and pd_upper fields are masked separately since they become meaningless for deleted pages
- Only used by index access methods that recycle deleted pages
- Essential for consistency checks of index structures that undergo page recycling operations
- The function preserves only the most basic page header structure while obliterating all content and space management information