# PageHasFreeLinePointers

## Location
[src/include/storage/bufpage.h:395-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L395-L399)

## Overview
Checks whether a page has free line pointers available for reuse by examining the PD_HAS_FREE_LINES flag in the page header.

## Definition

```c
static inline bool
PageHasFreeLinePointers(Page page)
```
## Detailed Description
This function is a simple flag checker that tests the PD_HAS_FREE_LINES bit in the page header's pd_flags field. When this flag is set, it indicates that the page contains line pointers that have been marked as unused and can be reclaimed for new tuple insertions. This optimization helps avoid unnecessary page fragmentation by reusing existing line pointer slots instead of always appending new ones.

The function operates on the page header structure, which contains metadata about the page including various status flags. The PD_HAS_FREE_LINES flag is one of several page-level hints that PostgreSQL maintains to optimize storage operations.

## Parameters / Member Variables
- `page`: A pointer to the page whose free line pointer status is being checked
## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header structure)
  - PD_HAS_FREE_LINES (flag constant)
- Called from (representative examples):
  - [PageAddItemExtended](PageAddItemExtended.md) (src/backend/storage/page/bufpage.c:251)
  - [PageGetHeapFreeSpace](PageGetHeapFreeSpace.md) (src/backend/storage/page/bufpage.c:1007)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- The function only checks the flag state and does not modify the page
- Used as part of PostgreSQL's space management strategy to efficiently reuse freed line pointers
- The flag is managed by PageSetHasFreeLinePointers and PageClearHasFreeLinePointers functions

## Simplified Source

```c
static inline bool
PageHasFreeLinePointers(Page page)
{
    // Check if page has reusable line pointers
    return ((PageHeader) page)->pd_flags & PD_HAS_FREE_LINES;
}
```