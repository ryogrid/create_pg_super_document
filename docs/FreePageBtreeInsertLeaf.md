# FreePageBtreeInsertLeaf

## Overview
Executes structured insertion of a new free page span entry into a Free Page Manager B-tree leaf node, maintaining sorted order through precise array manipulation and preserving all B-tree structural invariants. This function serves as the fundamental data insertion mechanism for leaf pages within PostgreSQL's memory management system, enabling efficient storage and retrieval of free page span information critical for dynamic memory allocation operations.

## Definition
```c
static void FreePageBtreeInsertLeaf(FreePageBtree *btp, Size index, Size first_page,
                                   Size npages)
```

## Detailed Description
FreePageBtreeInsertLeaf implements the essential leaf-level insertion algorithm within PostgreSQL's Free Page Manager B-tree infrastructure, handling the precise task of maintaining sorted key order while adding new free page span entries. The function performs rigorous validation through assertion checking to confirm the target page is a valid leaf node with sufficient capacity and that the insertion index falls within proper bounds. The insertion process involves systematically shifting existing leaf entries rightward using memmove to create the necessary space at the target index, then populating the new entry with both the starting page number (first_page) and span size (npages) to completely define the free memory region. The operation completes by incrementing the leaf's usage count, ensuring accurate metadata for subsequent search and allocation operations. This function plays a crucial role in the Free Page Manager's ability to track and manage available memory spans, directly supporting PostgreSQL's dynamic memory allocation requirements across various workload patterns.

## Parameters / Member Variables
- `btp`: Pointer to the target FreePageBtree leaf page where the new span entry will be inserted, must have FREE_PAGE_LEAF_MAGIC and available capacity for additional entries
- `index`: Zero-based insertion position within the leaf_key array, must be ≤ current nused value to maintain proper array bounds and sorted key order
- `first_page`: Starting page number of the free memory span being inserted, serves as the primary sort key for B-tree ordering and search operations
- `npages`: Size of the free memory span in page units, defining the total amount of contiguous memory available starting from first_page

## Dependencies
- **Functions called/Symbols referenced**:
  - `FREE_PAGE_LEAF_MAGIC` - Magic number constant used for validation to ensure the target page is a leaf node rather than an internal navigation node
  - `FPM_ITEMS_PER_LEAF_PAGE` - Capacity constant defining the maximum number of leaf entries that can be stored within a single leaf page
  - `FreePageBtreeLeafKey` - Structure type defining the format for leaf entries containing both first_page and npages information
  - `memmove` - Memory manipulation function providing safe array element shifting to create insertion space without data corruption
- **Called from (representative examples)**:
  - `FreePageManagerPutInternal` - Uses leaf insertion during memory deallocation operations when returning free page spans to the allocation system
  - Memory consolidation operations - Called when combining adjacent free spans or splitting larger spans during allocation optimization

## Notes & Other Information
This function embodies PostgreSQL's commitment to efficient memory management through sophisticated data structure maintenance. The insertion algorithm is designed to maintain B-tree ordering invariants while providing optimal performance for common insertion patterns. The use of memmove ensures safe memory manipulation even when memory regions overlap, which is critical for insertions at intermediate array positions. The function's simplicity belies its importance in the overall memory management architecture - virtually all memory deallocation operations ultimately depend on this leaf insertion mechanism. Performance characteristics are excellent for end-of-array insertions (common case) and acceptable for middle insertions due to the memmove overhead. The assertion checking provides valuable debugging support during development while being optimized out in production builds. Thread safety is managed through the Free Page Manager's broader locking protocols, ensuring that concurrent access to leaf pages doesn't result in data corruption. The function assumes that callers have performed appropriate duplicate checking and span validation to maintain the integrity of the free page tracking system.