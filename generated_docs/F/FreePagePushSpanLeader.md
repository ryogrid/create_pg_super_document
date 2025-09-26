# FreePagePushSpanLeader

## Location
[src/backend/utils/mmgr/freepage.c:1871-1886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1871-L1886)

## Overview
Initializes a new FreePageSpanLeader structure and inserts it into the appropriate free list within the FreePageManager to track available page spans.

## Definition

```c
static void
FreePagePushSpanLeader(FreePageManager *fpm, Size first_page, Size npages)
```
## Detailed Description
This static function creates and initializes a new FreePageSpanLeader structure to represent a contiguous span of free pages. The function performs several key operations:

1. Calculates which free list index to use based on the number of pages (npages), capped at FPM_NUM_FREELISTS-1
2. Converts the first page number to a pointer using the segment base address
3. Initializes the span leader structure with magic number, page count, and proper linkage
4. Inserts the new span at the head of the appropriate free list using doubly-linked list operations
5. Updates both forward and backward pointers to maintain list integrity

The function is part of PostgreSQL's free page management system, which organizes available memory pages into multiple free lists based on span size for efficient allocation and deallocation.

## Parameters / Member Variables
- : Pointer to the FreePageManager structure that manages the free page lists
- : The page number of the first page in the span to be added to the free list
- : The number of contiguous pages in this free span

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_page_to_pointer
  - relptr_access
  - relptr_store
  - Min
- Types used:
  - FreePageManager
  - FreePageSpanLeader
  - Size
- Constants used:
  - FPM_NUM_FREELISTS
  - FREE_PAGE_SPAN_LEADER_MAGIC
- Called from (representative examples):
  - FreePageBtreeCleanup
  - FreePageManagerGetInternal
  - FreePageManagerPutInternal

## Notes and Other Information
- This is a static function, only accessible within the freepage.c module
- The function uses relative pointers (relptr) for memory management across different address spaces
- The free list index is calculated as Min(npages, FPM_NUM_FREELISTS) - 1, meaning spans larger than FPM_NUM_FREELISTS pages all go into the same highest-indexed list
- The span leader structure is placed at the beginning of the first free page in the span
- Proper doubly-linked list maintenance ensures O(1) insertion time and maintains list integrity
- The magic number FREE_PAGE_SPAN_LEADER_MAGIC helps with debugging and corruption detection