# _hash_saveitem

## Location
src/backend/access/hash/hashsearch.c: 708 - 715

## Overview
Saves an index tuple's essential information (heap TID and index offset) into the scan's current position buffer at a specified index location.

## Definition
```c
static inline void _hash_saveitem(HashScanOpaque so, int itemIndex, OffsetNumber offnum, IndexTuple itup)
```

## Detailed Description
This inline helper function extracts and stores the critical information from a qualifying index tuple into the scan's current position buffer. It specifically saves the heap tuple identifier (TID) from the index tuple and the offset number where the tuple was found on the index page. This information is essential for later retrieving the actual heap tuple when the scan results are processed.

The function provides a simple but crucial abstraction for storing scan results, ensuring that the scan can efficiently maintain a collection of qualifying tuples without having to store the entire index tuple data.

## Parameters / Member Variables
- `so`: HashScanOpaque structure containing the scan state and current position buffer
- `itemIndex`: Index position in the items array where this tuple information should be stored
- `offnum`: Offset number of the tuple on the index page
- `itup`: Index tuple containing the heap TID and other index information

## Dependencies
- Functions called/Symbols referenced:
  - [HashScanPosItem](../H/HashScanPosItem.md) (struct type)
- Called from (representative examples):
  - [_hash_load_qualified_items](_hash_load_qualified_items.md)

## Notes and Other Information
- Declared as static inline for performance optimization due to its frequent usage
- Only stores essential information (heapTid and indexOffset) rather than the full index tuple
- The heapTid (t_tid) is used to locate the corresponding tuple in the heap table
- The indexOffset is used to identify the specific location of the tuple on the index page
- Part of the hash index scanning infrastructure that enables efficient batch processing of scan results