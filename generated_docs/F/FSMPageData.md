# FSMPageData

## Location
[src/include/storage/fsm_internals.h:43-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/fsm_internals.h#L43-L44)

## Overview
FSMPageData is a structure representing the layout of a Free Space Map (FSM) page, which stores information about available free space in heap pages using a binary tree structure for efficient search and update operations.

## Definition


## Detailed Description
FSMPageData defines the internal structure of a Free Space Map page, which is part of PostgreSQL's free space management system. The FSM is used to quickly locate pages with enough free space to store new tuples, or to determine if relation extension is needed.

The structure implements a binary tree stored as an array within each FSM page. Leaf nodes store the amount of free space on corresponding heap pages (measured in 1/256th granularity of a page), while non-leaf nodes store the maximum free space value among their children. This tree structure allows for efficient searching (O(log n)) to find pages with sufficient free space and quick updates when page free space changes.

The binary tree is not perfect due to page header overhead, meaning some right-most leaf nodes are missing, but the tree is guaranteed to be complete above the leaf level. The structure supports both search operations (finding pages with X bytes of free space) and update operations (setting free space on a page and bubbling changes up the tree).

## Parameters / Member Variables
- : An integer pointer to the next slot to be returned in round-robin fashion when multiple backends are searching for free space concurrently. This helps distribute load across different backends and reduces contention while maintaining sequential page filling for OS prefetching benefits. Defined as int rather than uint16 for atomic fetch/store operations even without exclusive locking.
- : A flexible array member containing the binary tree structure stored as an array. The first NonLeafNodesPerPage elements are upper (internal) nodes, followed by LeafNodesPerPage elements as leaf nodes. Unused nodes are set to zero. Each element is a uint8 representing free space in 1/256th granularity.

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member syntax)
  - BLCKSZ (PostgreSQL block size constant)
  - MAXALIGN (memory alignment macro)
  - SizeOfPageHeaderData (page header size)

- Called from (representative examples):
  - FSMPage (typedef pointer to FSMPageData)
  - NodesPerPage (calculation macro using FSMPageData structure)
  - fsm_search_avail() (searches within FSM page structure)
  - fsm_set_avail() (updates FSM page structure)
  - fsm_get_avail() (retrieves values from FSM page structure)

## Notes and Other Information
- [FSMPageData](FSMPageData.md) is the fundamental building block of PostgreSQL's scalable Free Space Map implementation introduced in version 8.4
- The structure supports up to 2^32-1 heap pages through a three-level tree hierarchy
- Free space is quantized to 256 levels (0-255) where each unit represents BLCKSZ/256 bytes
- The fp_next_slot field is updated even under shared locks as it serves as a hint and corruption can be easily recovered
- FSM pages are not explicitly WAL-logged; instead, the system relies on self-correcting measures and periodic VACUUM updates
- The binary tree layout optimizes for both search performance and load distribution across concurrent backends
- Related constants: NodesPerPage, NonLeafNodesPerPage, LeafNodesPerPage, and SlotsPerFSMPage define the page capacity calculations