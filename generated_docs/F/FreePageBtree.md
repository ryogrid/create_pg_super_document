# FreePageBtree

## Location
[src/backend/utils/mmgr/freepage.c:108-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L108-L118)

## Overview
FreePageBtree represents a complete btree page that can function as either an internal or leaf node, providing the unified structure for PostgreSQL's free page management btree.

## Definition

```c
struct FreePageBtree
{
	FreePageBtreeHeader hdr;
	union
	{
		FreePageBtreeInternalKey internal_key[FPM_ITEMS_PER_INTERNAL_PAGE];
		FreePageBtreeLeafKey leaf_key[FPM_ITEMS_PER_LEAF_PAGE];
	}			u;
};
```
## Detailed Description
The FreePageBtree structure represents a complete btree page in PostgreSQL's free page management system. This unified design allows a single structure to serve as either an internal node (for navigation) or a leaf node (containing actual data) depending on the magic value in the header. The structure consists of a common header followed by a union that contains either an array of internal keys or leaf keys.

The btree implementation provides efficient organization of free memory spans, enabling logarithmic-time searches, insertions, and deletions. Internal pages contain navigation keys that direct searches to appropriate child pages, while leaf pages contain the actual free span information. The capacity of each page type is calculated based on the available space after accounting for the header.

This btree serves as the primary data structure for tracking and allocating free memory spans larger than what can be efficiently managed by simple freelists.

## Parameters / Member Variables
- : Common header containing magic number (page type), usage count, and parent pointer
- : Array of internal keys for navigation when page serves as internal node (capacity: FPM_ITEMS_PER_INTERNAL_PAGE)
- : Array of leaf keys containing free span data when page serves as leaf node (capacity: FPM_ITEMS_PER_LEAF_PAGE)

## Dependencies
- Functions called/Symbols referenced:
  - FreePageBtreeHeader (common page header structure)
  - FreePageBtreeInternalKey (internal node key structure)
  - FreePageBtreeLeafKey (leaf node key structure)
  - FPM_ITEMS_PER_INTERNAL_PAGE (internal page capacity constant)
  - FPM_ITEMS_PER_LEAF_PAGE (leaf page capacity constant)

- Called from (representative examples):
  - FreePageBtreeSearch (btree search operations)
  - FreePageBtreeInsertInternal (internal key insertion)
  - FreePageBtreeInsertLeaf (leaf key insertion)
  - FreePageBtreeRemove (key removal operations)
  - FreePageBtreeSplitPage (page splitting during growth)
  - FreePageBtreeConsolidate (btree balancing operations)
  - FreePageManagerPutInternal (deallocation operations)
  - FreePageManagerDumpBtree (debugging and inspection)

## Notes and Other Information
- Serves dual purpose as both internal and leaf btree pages through union design
- Page type determined by magic number in the header (FREE_PAGE_INTERNAL_MAGIC vs FREE_PAGE_LEAF_MAGIC)
- Capacity calculations ensure optimal utilization of the fixed page size (FPM_PAGE_SIZE = 4096 bytes)
- Essential component of the larger free page management system for efficient memory allocation
- Supports standard btree operations including search, insert, delete, split, and merge
- Used in conjunction with freelists for complete memory management coverage
- Enables efficient allocation of variable-sized memory spans through btree organization
- Critical for performance in systems with complex memory allocation patterns
- Part of PostgreSQL's sophisticated memory management infrastructure for shared memory segments