# fsm_get_avail

## Location
[src/backend/storage/freespace/fsmpage.c:122-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/fsmpage.c#L122-L137)

## Overview
The fsm_get_avail function retrieves the free space value for a specific slot from a Free Space Map page without requiring any locks.

## Definition
```c
uint8 fsm_get_avail(Page page, int slot)
```

## Detailed Description
This function provides read-only access to the free space value stored in a specific slot of a Free Space Map page. It directly accesses the leaf node in the binary tree structure that corresponds to the given slot number. The function is designed to be lightweight and lock-free since it only performs a single byte read operation.

The function converts the slot number to the corresponding leaf node index by adding NonLeafNodesPerPage to the slot number, then retrieves the value from the fp_nodes array in the FSMPage structure.

## Parameters / Member Variables
- `page`: The Free Space Map page to read from (no locking required)
- `slot`: The slot number to query (must be less than LeafNodesPerPage)

## Dependencies
- Functions called/Symbols referenced:
  - `[PageGetContents](../P/PageGetContents.md)`: Extracts page contents as FSMPage structure
  - `FSMPage`: Type representing Free Space Map page data
  - `LeafNodesPerPage`: Constant defining number of leaf nodes per page
  - `NonLeafNodesPerPage`: Constant defining number of non-leaf nodes (used to calculate leaf node offset)
- Called from (representative examples):
  - `[GetRecordedFreeSpace](../G/GetRecordedFreeSpace.md)`: Retrieves recorded free space information
  - `[fsm_vacuum_page](fsm_vacuum_page.md)`: Reads current free space values during vacuum operations

## Notes and Other Information
- This is a read-only function that doesn't require page locking due to single-byte atomic access
- Returns a uint8 value representing the amount of free space available in the slot
- Includes assertion to validate that the slot number is within valid range
- Part of PostgreSQL's Free Space Map system for efficient space management
- The function is optimized for performance since it's frequently called during space searches
- Direct array access makes this operation very fast compared to tree traversal methods

## Simplified Source

```c
uint8 fsm_get_avail(Page page, int slot) {
    // Extract FSM page structure from the page
    FSMPage fsmpage = (FSMPage) PageGetContents(page);

    // Validate slot is within valid range
    Assert(slot < LeafNodesPerPage);

    // Return free space value for this slot
    // Leaf nodes start after NonLeafNodesPerPage
    return fsmpage->fp_nodes[NonLeafNodesPerPage + slot];
}
```