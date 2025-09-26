# FreePageSpanLeader

## Location
[src/backend/utils/mmgr/freepage.c:68-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L68-L76)

## Overview
FreePageSpanLeader represents a doubly linked list node that tracks contiguous spans of free pages in PostgreSQL's free page manager, stored in the first page of each free span.

## Definition

```c
struct FreePageSpanLeader
{
	int			magic;			/* always FREE_PAGE_SPAN_LEADER_MAGIC */
	Size		npages;			/* number of pages in span */
	RelptrFreePageSpanLeader prev;
	RelptrFreePageSpanLeader next;
};
```
## Detailed Description
The FreePageSpanLeader structure serves as a header for contiguous spans of free pages within PostgreSQL's memory management system. Each free page span begins with this structure stored in the first page of the span, creating a doubly-linked list of all free spans. This design allows efficient tracking and management of variable-sized free memory regions.

The structure uses relative pointers (RelptrFreePageSpanLeader) for the linked list connections, which enables the free page manager to work correctly even when the memory segment is mapped at different virtual addresses across processes or when relocated.

## Parameters / Member Variables
- : Magic number constant (FREE_PAGE_SPAN_LEADER_MAGIC) used for validation and debugging
- : Number of contiguous pages in this free span, including the leader page itself
- : Relative pointer to the previous span leader in the doubly-linked list
- : Relative pointer to the next span leader in the doubly-linked list

## Dependencies
- Functions called/Symbols referenced:
  - Size (PostgreSQL size type)
  - RelptrFreePageSpanLeader (relative pointer type)

- Called from (representative examples):
  - FreePageManagerInitialize
  - FreePageManagerGetInternal
  - FreePagePopSpanLeader
  - FreePagePushSpanLeader
  - FreePageBtreeGetRecycled
  - FreePageBtreeRecycle

## Notes and Other Information
- The structure is stored at the beginning of the first page in each free span
- Forms part of PostgreSQL's free page management system that uses 4KB pages (FPM_PAGE_SIZE)
- The doubly-linked list design allows for efficient insertion and removal of spans
- Used in conjunction with freelists organized by span size for different allocation strategies
- The magic number provides corruption detection and debugging capabilities
- Critical component in the memory allocation subsystem for shared memory segments