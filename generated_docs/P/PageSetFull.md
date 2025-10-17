# PageSetFull

## Location
[src/include/storage/bufpage.h:416-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L416-L420)

## Overview
Sets the PD_PAGE_FULL flag in the page header to mark the page as full and unsuitable for new tuple insertions.

## Definition
static inline void PageSetFull(Page page)

## Detailed Description
This function sets the PD_PAGE_FULL bit in the page header's pd_flags field using a bitwise OR operation. This flag serves as a performance optimization hint indicating that the page has insufficient space for new tuple insertions. By marking pages as full, PostgreSQL can avoid expensive space calculations and insertion attempts on pages that are known to be at or near capacity.

The function is typically called when an insertion operation determines that a page lacks adequate space, either through direct space calculation or after a failed insertion attempt. Setting this flag helps subsequent operations quickly identify and skip over full pages during space allocation.

## Parameters / Member Variables
- page: A pointer to the page that should be marked as full

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header structure)
  - PD_PAGE_FULL (flag constant)
- Called from (representative examples):
  - [heap_update](../h/heap_update.md) (src/backend/access/heap/heapam.c:3992)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- The function modifies the page header by setting a specific bit flag
- Used in conjunction with PageIsFull for complete page fullness management
- Primarily called during heap update operations when space constraints are encountered
- Helps optimize insertion performance by preventing repeated attempts on pages with insufficient space
- The flag serves as a hint and may be cleared during page maintenance operations if space becomes available

## Simplified Source

```c
static inline void
PageSetFull(Page page)
{
    // Set the PD_PAGE_FULL flag in the page header
    ((PageHeader) page)->pd_flags |= PD_PAGE_FULL;
}
```