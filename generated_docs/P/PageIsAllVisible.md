# PageIsAllVisible

## Location
[src/include/storage/bufpage.h:427-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L427-L431)

## Overview
PageIsAllVisible checks whether all tuples on a page are visible to everyone, indicating that the page can be safely marked in the visibility map.

## Definition
static inline bool PageIsAllVisible(Page page)

## Detailed Description
PageIsAllVisible is an inline function that examines the PD_ALL_VISIBLE flag in a page's header to determine if all tuples on the page are visible to all transactions. This flag is used as an optimization for vacuum operations and visibility map management. When this flag is set, it indicates that all tuples on the page are committed and visible to all current and future transactions, making the page a candidate for being marked as all-visible in the visibility map.

The function performs a simple bitwise AND operation between the page's pd_flags field and the PD_ALL_VISIBLE constant to test if this specific flag bit is set.

## Parameters / Member Variables
- page: A pointer to the page (Page type) to check for the all-visible status

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - PD_ALL_VISIBLE (flag constant 0x0004)
- Called from (representative examples):
  - [heap_prepare_pagescan](../h/heap_prepare_pagescan.md)
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [lazy_scan_prune](../l/lazy_scan_prune.md)
  - [visibilitymap_set](../v/visibilitymap_set.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The PD_ALL_VISIBLE flag value is 0x0004
- Used extensively throughout heap operations to determine visibility map updates
- Critical for vacuum optimizations and index-only scans
- The flag helps determine when pages can be skipped during vacuum operations
- Frequently used in heap access methods and vacuum lazy operations
- Returns true if all tuples on the page are visible to everyone, false otherwise

## Simplified Source

```c
static inline bool
PageIsAllVisible(Page page)
{
    // Check if the PD_ALL_VISIBLE flag is set in the page header
    return ((PageHeader) page)->pd_flags & PD_ALL_VISIBLE;
}
```