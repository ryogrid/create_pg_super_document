# FreePageBtreeLeafKey

## Location
src/backend/utils/mmgr/freepage.c: 93 - 97

## Overview
FreePageBtreeLeafKey represents the actual data entries in leaf pages of the free page btree, containing the location and size information of free memory spans.

## Definition

```c
typedef struct FreePageBtreeLeafKey
{
	Size		first_page;		/* first page in span */
	Size		npages;			/* number of pages in span */
} FreePageBtreeLeafKey;
```
## Detailed Description
The FreePageBtreeLeafKey structure represents the terminal entries in the free page btree, containing the actual data about available free memory spans. Each leaf key identifies a contiguous span of free pages by specifying its starting page number and length. These keys are stored in sorted order within leaf pages, enabling efficient range searches and allocation operations.

Unlike internal keys which only store boundary information for navigation, leaf keys contain the complete information needed to fulfill memory allocation requests. The btree structure ensures that these leaf keys can be efficiently located and managed as free spans are allocated and returned to the system.

The structure is designed to be compact, containing only the essential information needed to identify and allocate free memory spans, with no additional payload data.

## Parameters / Member Variables
- : The page number of the first page in the free span, serving as both the search key and location identifier
- : The total number of contiguous pages in this free span, indicating the size available for allocation

## Dependencies
- Functions called/Symbols referenced:
  - Size (PostgreSQL size type for page numbers and counts)

- Called from (representative examples):
  - FPM_ITEMS_PER_LEAF_PAGE (capacity calculation macro)
  - FreePageBtree (union member in btree page structure)
  - FreePageBtreeConsolidate (btree maintenance and merging operations)
  - FreePageBtreeInsertLeaf (insertion into leaf pages)
  - FreePageBtreeRemove (removal of free spans)
  - FreePageBtreeRemovePage (page removal operations)
  - FreePageBtreeSplitPage (page splitting during insertion)
  - FreePageManagerGetInternal (allocation operations)
  - FreePageManagerPutInternal (deallocation operations)

## Notes and Other Information
- Contains the actual free span data rather than just navigation information
- Enables efficient searching and allocation of free memory spans by size and location
- The first_page field serves dual purposes as both search key and location identifier
- Sorted by first_page within leaf pages to maintain btree ordering properties
- Essential for implementing first-fit, best-fit, or other allocation strategies
- Used in both allocation (finding suitable spans) and deallocation (returning spans) operations
- Works together with FreePageSpanLeader structures which are stored at the actual memory locations
- Critical component in PostgreSQL's memory management system for dynamic segment allocation