# PageClearAllVisible

## Location
[src/include/storage/bufpage.h:437-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L437-L444)

## Overview
PageClearAllVisible clears the PD_ALL_VISIBLE flag from a page header, indicating that not all tuples on the page are visible to everyone.

## Definition
static inline void PageClearAllVisible(Page page)

## Detailed Description
PageClearAllVisible is an inline function that removes the PD_ALL_VISIBLE flag from a page's header flags field. This function is called when modifications are made to a page that invalidate the assumption that all tuples on the page are visible to all transactions. This typically happens during insert, update, or delete operations that add new tuples or modify existing ones in ways that make them not immediately visible to all transactions.

The function uses a bitwise AND operation with the complement of PD_ALL_VISIBLE to clear only that specific flag bit while preserving all other flags in the page header. This ensures that the visibility map will be updated accordingly to reflect that the page is no longer all-visible.

## Parameters / Member Variables
- page: A pointer to the page (Page type) whose PD_ALL_VISIBLE flag should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - PD_ALL_VISIBLE (flag constant 0x0004)
- Called from (representative examples):
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md)
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [heap_xlog_delete](../h/heap_xlog_delete.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md)
  - [heap_xlog_update](../h/heap_xlog_update.md)
  - [lazy_scan_prune](../l/lazy_scan_prune.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Called frequently during heap modification operations
- Critical for maintaining visibility map accuracy
- Used in both normal operations and WAL replay functions
- Ensures that pages with new or modified tuples are not incorrectly marked as all-visible
- The function uses bitwise operations to efficiently clear only the target flag bit
- Often triggers subsequent visibility map updates to clear the corresponding visibility map bit
- Essential for correctness of index-only scans and vacuum optimizations

## Simplified Source

```c
static inline void
PageClearAllVisible(Page page)
{
    // Clear the PD_ALL_VISIBLE flag using bitwise AND with complement
    ((PageHeader) page)->pd_flags &= ~PD_ALL_VISIBLE;
}
```