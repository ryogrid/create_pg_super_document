# FreePageBtreeInternalKey

## Location
src/backend/utils/mmgr/freepage.c: 86 - 90

## Overview
FreePageBtreeInternalKey represents an entry in internal (non-leaf) btree pages, providing navigation to child pages with a key boundary for efficient btree traversal.

## Definition

```c
typedef struct FreePageBtreeInternalKey
{
	Size		first_page;		/* low bound for keys on child page */
	RelptrFreePageBtree child;	/* downlink */
} FreePageBtreeInternalKey;
```
## Detailed Description
The FreePageBtreeInternalKey structure serves as an internal node entry in the free page btree, implementing the standard btree internal key format. Each internal key defines a boundary value (first_page) that represents the minimum key value that can be found in the corresponding child subtree. This enables efficient btree search operations by directing traversal to the appropriate child page based on the search key.

The structure uses relative pointers for child references, allowing the btree to function correctly even when memory segments are mapped at different virtual addresses. Internal keys are stored as arrays within internal btree pages, with the number of keys per page calculated based on available space after accounting for the page header.

## Parameters / Member Variables
- : The minimum page number (key value) that can be found in the child subtree rooted at the corresponding child page
- : Relative pointer to the child btree page that contains keys greater than or equal to first_page

## Dependencies
- Functions called/Symbols referenced:
  - Size (PostgreSQL size type for page numbers)
  - RelptrFreePageBtree (relative pointer to btree page)

- Called from (representative examples):
  - FPM_ITEMS_PER_INTERNAL_PAGE (capacity calculation macro)
  - FreePageBtree (union member in btree page structure)
  - FreePageBtreeConsolidate (btree maintenance operation)
  - FreePageBtreeInsertInternal (insertion into internal pages)
  - FreePageBtreeRemovePage (page removal operations)
  - FreePageBtreeSplitPage (page splitting during insertion)

## Notes and Other Information
- Forms the navigation structure for internal btree pages in the free page management system
- The first_page value serves as a search key boundary for efficient btree traversal
- Part of a B-tree implementation that organizes free memory spans by size for optimal allocation
- Works in conjunction with FreePageBtreeLeafKey for complete btree functionality
- Critical for maintaining btree structure during insertion, deletion, and balancing operations
- Enables logarithmic-time searches through the free page space
- Used extensively in btree maintenance operations like page splitting and consolidation