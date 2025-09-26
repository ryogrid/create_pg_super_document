# FreePageManager

## Location
[src/include/utils/freepage.h:48-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/freepage.h#L48-L66)

## Overview
FreePageManager is a structure that manages free memory pages in PostgreSQL, providing infrastructure for memory allocators that need to work with relative pointers and page-organized memory management.

## Definition

```c
struct FreePageManager
{
	RelptrFreePageManager self;
	RelptrFreePageBtree btree_root;
	RelptrFreePageSpanLeader btree_recycle;
	unsigned	btree_depth;
	unsigned	btree_recycle_count;
	Size		singleton_first_page;
	Size		singleton_npages;
	Size		contiguous_pages;
	bool		contiguous_pages_dirty;
	RelptrFreePageSpanLeader freelist[FPM_NUM_FREELISTS];
#ifdef FPM_EXTRA_ASSERTS
	/* For debugging only, pages put minus pages gotten. */
	Size		free_pages;
#endif
};
```
## Detailed Description
FreePageManager keeps track of which 4kB pages of memory are currently unused from the perspective of higher-level memory allocators. Unlike user-facing allocators like palloc(), it can only allocate and free in units of whole pages, requiring knowledge of allocation length for freeing operations.

The manager uses a sophisticated approach combining multiple freelists and a btree structure:
- Multiple freelists store runs of pages sorted by size, with the first 128 lists containing spans of specific sizes and the last list containing everything larger
- An in-memory btree of free page ranges ordered by page number helps consolidate adjacent spans to avoid fragmentation
- When there's only one range of free pages, the btree is trivial and stored within the FreePageManager itself
- For multiple ranges, pages are allocated from the managed area as needed for btree storage

The system uses relative pointers throughout to support dynamic shared memory scenarios where absolute pointers are not viable.

## Parameters / Member Variables
- `self`: Relative pointer to this FreePageManager structure itself
- `btree_root`: Root of the btree containing free page ranges ordered by page number
- `btree_recycle`: Linked list of btree pages available for recycling
- `btree_depth`: Current depth of the btree structure
- `btree_recycle_count`: Number of btree pages available for recycling
- `singleton_first_page`: First page number when there's only one contiguous range
- `singleton_npages`: Number of pages when there's only one contiguous range
- `contiguous_pages`: Size of the largest contiguous run of free pages
- `contiguous_pages_dirty`: Flag indicating if contiguous_pages needs recalculation
- `freelist[FPM_NUM_FREELISTS]`: Array of 129 freelists for different span sizes
- `free_pages`: Debug-only counter tracking pages put minus pages gotten

## Dependencies
- Functions called/Symbols referenced:
  - [FreePageSpanLeader](FreePageSpanLeader.md)
  - [FreePageBtree](FreePageBtree.md)
  - RelptrFreePageManager
  - RelptrFreePageBtree
  - RelptrFreePageSpanLeader
- Called from (representative examples):
  - [dsm_shmem_init](../d/dsm_shmem_init.md) (DSM shared memory initialization)
  - [dsm_create](../d/dsm_create.md) (DSM segment creation)
  - [dsa_minimum_size](../d/dsa_minimum_size.md) (DSA minimum size calculation)
  - [create_internal](../c/create_internal.md) (DSA area creation)
  - [attach_internal](../a/attach_internal.md) (DSA area attachment)

## Notes and Other Information
- Uses 4kB pages (FPM_PAGE_SIZE) rather than PostgreSQL's typical 8kB pages to align with common OS memory allocation page sizes
- Designed specifically for dynamic shared memory management where relative pointers are essential
- Implements sophisticated memory consolidation to prevent fragmentation through btree-based range tracking
- The freelist design allows small allocations to simply pop from the appropriate list without size checking
- Includes debugging assertions when FPM_EXTRA_ASSERTS is defined to track allocation/deallocation balance