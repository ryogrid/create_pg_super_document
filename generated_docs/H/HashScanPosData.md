# HashScanPosData

## Location
src/include/access/hash.h: 109 - 128

## Overview
HashScanPosData is a structure that maintains the state and position information for hash index scans, including page navigation data and an array of matched items.

## Definition


## Detailed Description
HashScanPosData is the core structure for managing hash index scan state. It tracks the current position within the bucket chain and maintains an array of items found on the current page. The structure supports both forward and backward scanning by providing flexible indexing mechanisms for the items array.

The design efficiently handles bucket chain traversal by maintaining links to previous and next pages, while the items array provides a buffer for matched tuples on the current page. This approach minimizes page access overhead during scan operations by processing multiple items per page visit.

## Parameters / Member Variables
- : Buffer reference for the currently pinned page (if valid)
- : Block number of the current hash index page being scanned
- : Block number of the next overflow page in the bucket chain
- : Block number of the previous page (either overflow or bucket page)
- : Index of the first valid entry in the items array
- : Index of the last valid entry in the items array
- : Cursor indicating which entry was last returned to the caller
- : Array of HashScanPosItem structures containing matched items (must be the last field)

## Dependencies
- Functions called/Symbols referenced:
  - HashScanPosItem
  - MaxIndexTuplesPerPage
  - Buffer
  - BlockNumber
- Called from (representative examples):
  - HashScanOpaqueData

## Notes and Other Information
The items array is always kept in index order (increasing indexoffset), but can be filled in different directions depending on scan direction. For backward scans, the array is filled from back to front for efficiency. The requirement that items[] be the last field in the structure is likely related to potential variable-length allocation strategies or memory layout optimizations. The dual indexing system (firstItem/lastItem plus itemIndex) provides flexible navigation through the matched items while supporting bidirectional scanning operations.