# FreePageBtreeHeader

## Location
src/backend/utils/mmgr/freepage.c: 77 - 83

## Overview
FreePageBtreeHeader serves as the common header structure for both internal and leaf pages in the free page btree, providing essential metadata and navigation information.

## Definition

```c
typedef struct FreePageBtreeHeader
{
	int			magic;			/* FREE_PAGE_LEAF_MAGIC or
								 * FREE_PAGE_INTERNAL_MAGIC */
	Size		nused;			/* number of items used */
	RelptrFreePageBtree parent; /* uplink */
} FreePageBtreeHeader;
```
## Detailed Description
The FreePageBtreeHeader structure provides common metadata and navigation capabilities for btree nodes in PostgreSQL's free page management system. This header appears at the beginning of both internal and leaf btree pages, establishing a consistent format for btree operations.

The magic field distinguishes between different types of btree pages (internal vs leaf), enabling type-safe operations and debugging. The nused field tracks how many entries are currently stored in the page, which is essential for insertion, deletion, and search operations. The parent pointer maintains upward navigation capability in the btree structure.

The header is followed by a variable-length array of keys, with the specific key type determined by the page type indicated in the magic field.

## Parameters / Member Variables
- : Magic number identifying page type (FREE_PAGE_LEAF_MAGIC for leaf pages, FREE_PAGE_INTERNAL_MAGIC for internal pages)
- : Current number of active items stored in this btree page
- : Relative pointer to the parent btree page, enabling upward traversal

## Dependencies
- Functions called/Symbols referenced:
  - Size (PostgreSQL size type)
  - RelptrFreePageBtree (relative pointer to btree page)

- Called from (representative examples):
  - FPM_ITEMS_PER_INTERNAL_PAGE (macro calculating internal page capacity)
  - FPM_ITEMS_PER_LEAF_PAGE (macro calculating leaf page capacity)
  - FreePageBtree (main btree page structure)

## Notes and Other Information
- Used as the common header for both internal and leaf btree pages
- Enables calculation of available space for keys on each page type
- The magic field serves dual purposes: type identification and corruption detection
- Essential for btree navigation operations including search, insertion, and deletion
- Works with relative pointers to support memory segment relocation
- Part of the larger free page btree system that organizes free memory spans for efficient allocation
- Header size is factored into capacity calculations for determining maximum keys per page