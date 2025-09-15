# FreePageBtreeInsertInternal

## Overview
Performs structured insertion of a new key-child pointer pair into a Free Page Manager B-tree internal node at a specified index position. This function implements fundamental B-tree insertion logic for internal pages by maintaining sorted key order through array element shifting, ensuring that all B-tree invariants are preserved during tree construction and modification operations within PostgreSQL's sophisticated memory management infrastructure.

## Definition
```c
static void FreePageBtreeInsertInternal(char *base, FreePageBtree *btp, Size index,
                                       Size first_page, FreePageBtree *child)
```

## Detailed Description
FreePageBtreeInsertInternal implements the core insertion algorithm for internal B-tree nodes within PostgreSQL's Free Page Manager system, handling the complex task of maintaining sorted key order while inserting new navigation entries. The function performs comprehensive validation through assertion checking to ensure the target page is indeed an internal node, has sufficient capacity, and that the insertion index is within valid bounds. The insertion process involves carefully shifting existing key-child pairs rightward using memmove to create space at the target index, then populating the new entry with the provided first_page key and child pointer using relative pointer storage for cross-process compatibility. The operation concludes by incrementing the node's usage count, maintaining accurate metadata for subsequent B-tree operations. This function is critical for B-tree growth operations, including page splits, tree expansion, and structural rebalancing that occur during intensive memory allocation scenarios.

## Parameters / Member Variables
- `base`: Base address pointer of the shared memory segment containing the Free Page Manager structures, required for resolving relative pointers during child pointer storage operations
- `btp`: Pointer to the target FreePageBtree internal page where the new key-child pair will be inserted, must have FREE_PAGE_INTERNAL_MAGIC and sufficient capacity
- `index`: Zero-based insertion position within the internal_key array, must be ≤ current nused value to maintain array bounds and sorted order
- `first_page`: Key value representing the minimum page number for all keys in the child subtree, used for B-tree navigation and search operations
- `child`: Pointer to the child FreePageBtree page that will be referenced by the new internal key entry, enabling downward tree navigation

## Dependencies
- **Functions called/Symbols referenced**:
  - `FREE_PAGE_INTERNAL_MAGIC` - Magic number constant for validation ensuring the target page is an internal B-tree node rather than a leaf
  - `FPM_ITEMS_PER_INTERNAL_PAGE` - Capacity constant defining maximum number of key-child pairs that can be stored in an internal page
  - `FreePageBtreeInternalKey` - Structure type defining the key-child pair format used in internal B-tree nodes
  - `relptr_store` - Stores child pointer as relative pointer for cross-process shared memory compatibility and proper reference management
  - `memmove` - Memory manipulation function for safely shifting existing key entries to create insertion space
- **Called from (representative examples)**:
  - `FreePageManagerPutInternal` - Uses internal insertion during complex B-tree modification operations including page splits and tree rebalancing
  - B-tree split operations - Called during page splitting scenarios when internal nodes need additional key-child navigation entries

## Notes & Other Information
This function represents a fundamental building block of PostgreSQL's B-tree implementation within the Free Page Manager, designed for high performance and reliability in multi-process environments. The use of memmove ensures safe memory copying even when source and destination ranges overlap, which can occur during insertion at intermediate positions. The assertion checking provides robust debugging support and helps maintain B-tree invariants during development and testing. The relative pointer storage via relptr_store ensures that child references remain valid across process boundaries in shared memory environments, which is essential for PostgreSQL's multi-process architecture. Performance is optimized for common insertion patterns, with the memmove operation being the primary performance consideration for insertions at the beginning or middle of the key array. The function assumes that the caller has performed appropriate capacity checking and tree locking to prevent concurrent modifications that could violate B-tree consistency.