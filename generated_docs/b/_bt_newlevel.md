# _bt_newlevel

## Location
[src/backend/access/nbtree/nbtinsert.c:2444-2629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2444-L2629)

## Overview
Creates a new root level above the existing root page when a B-tree root split occurs, establishing a new tree level with downlink pointers to the split pages.

## Definition

```c
static Buffer
_bt_newlevel(Relation rel, Relation heaprel, Buffer lbuf, Buffer rbuf)
```
## Detailed Description
The  function is called during B-tree insertion when the root page needs to be split, requiring the creation of a new root page at a higher level. This operation is critical for maintaining B-tree balance and ensuring logarithmic search performance as the tree grows.

The function performs several key operations:
1. Allocates a new page to serve as the new root
2. Creates downlink index tuples pointing to the old root (left child) and its new sibling (right child)
3. Updates the B-tree metadata to reflect the new root and increased tree level
4. Handles WAL logging for crash recovery consistency
5. Uses critical sections to ensure atomicity of the multi-page update

The operation is designed to be deadlock-free by following a strict locking order: writers lock the root before the metadata page, while readers release metadata locks before attempting root locks.

## Parameters / Member Variables
- `rel`: The index relation being modified
- `heaprel`: The corresponding heap relation (used for space allocation)
- `lbuf`: Buffer containing the old root page (left child after split)
- `rbuf`: Buffer containing the new sibling page (right child after split)
## Dependencies
- Functions called/Symbols referenced:
  - : Allocates a new B-tree page
  - : Acquires a buffer for a specific page
  - : Updates metadata page format if needed
  - : Adds index tuples to the new root page
  - : Sets the downlink pointer in index tuples
  - : Records WAL entry for crash recovery
- Called from (representative examples):
  - : When inserting a new key requires root split

## Notes and Other Information
- The function operates within a critical section to ensure atomicity across multiple page updates
- Creates a "minus infinity" key for the left child downlink, ensuring it's less than any real key
- The right child downlink uses the high key from the original root page
- Updates both the regular root/level and fast root/level metadata fields
- Handles both old and new metadata page formats via version checking
- Returns the new root buffer, which the caller must unlock and unpin along with the child buffers