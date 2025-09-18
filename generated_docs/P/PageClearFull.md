# PageClearFull

## Location
[src/include/storage/bufpage.h:421-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L421-L426)

## Overview
PageClearFull clears the PD_PAGE_FULL flag bit from a page's header, indicating that the page may have sufficient free space for new tuple insertions.

## Definition
static inline void PageClearFull(Page page)

## Detailed Description
PageClearFull is an inline function that removes the PD_PAGE_FULL flag from a page's pd_flags field. The PD_PAGE_FULL flag is a hint that indicates when an UPDATE operation couldn't find enough free space in the page for its new tuple version, suggesting that a prune operation is needed. By clearing this flag, the function indicates that the page may now have sufficient space available, typically after operations like pruning or tuple removal that free up space.

This function operates directly on the page header's flags field using a bitwise AND operation with the complement of PD_PAGE_FULL to turn off only that specific flag bit while preserving all other flags.

## Parameters / Member Variables
- page: A pointer to the page (Page type) whose PD_PAGE_FULL flag should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - PD_PAGE_FULL (flag constant)
- Called from (representative examples):
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md)
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The PD_PAGE_FULL flag is considered a hint rather than absolute truth
- Changes to this flag are not WAL-logged as they represent optimization hints
- Typically called after operations that free up space on a page, such as pruning or tuple removal
- The function uses bitwise operations to efficiently clear only the target flag bit