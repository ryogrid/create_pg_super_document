# fsm_get_max_avail

## Location
src/backend/storage/freespace/fsmpage.c: 138 - 157

## Overview
The fsm_get_max_avail function returns the maximum available free space value stored in the root node of a Free Space Map page.

## Definition
```c
uint8 fsm_get_max_avail(Page page)
```

## Detailed Description
This function provides quick access to the maximum free space available on any slot within a Free Space Map page by reading the value stored in the root node of the binary tree structure. Since the tree maintains the invariant that each internal node contains the maximum value of its children, the root node (index 0) always contains the maximum value across all leaf nodes on the page.

This is an extremely efficient operation as it requires only a single array access to determine if the page has any slots that could accommodate a request of a given size, without needing to traverse the entire tree or examine individual slots.

## Parameters / Member Variables
- `page`: The Free Space Map page to query (no locking required)

## Dependencies
- Functions called/Symbols referenced:
  - `PageGetContents`: Extracts page contents as FSMPage structure
  - `FSMPage`: Type representing Free Space Map page data
- Called from (representative examples):
  - `fsm_search`: Uses max available to determine if page is worth searching
  - `fsm_vacuum_page`: Checks maximum available space during vacuum operations

## Notes and Other Information
- This is a read-only function that doesn't require page locking due to single-byte atomic access
- Returns a uint8 value representing the maximum free space available across all slots on the page
- The root node (index 0) always contains the maximum value due to the binary tree's max-heap property
- Part of PostgreSQL's Free Space Map system for efficient space management
- This function enables quick elimination of pages that cannot satisfy a space request
- Extremely fast operation making it suitable for frequent calls during space searches
- The value represents the largest amount of free space available in any single slot on the page