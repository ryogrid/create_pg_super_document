# getNextNearest

## Location
src/backend/access/gist/gistget.c: 560 - 611

## Overview
getNextNearest fetches the next heap tuple in an ordered GiST index scan by processing search queue items in distance order until a heap tuple is found.

## Definition
static bool getNextNearest(IndexScanDesc scan)

## Detailed Description
This function implements the core logic for ordered (nearest neighbor) searches in GiST indexes. It operates by:

1. **Queue Processing**: Continuously extracts items from the search queue using getNextGISTSearchItem(), which returns items in order based on their distances.

2. **Item Type Handling**: 
   - **Heap Items**: When a GISTSearchItem represents a heap tuple (identified by GISTSearchItemIsHeap), it sets up the scan result with the heap TID, recheck flag, distances, and optionally the reconstructed tuple for index-only scans.
   - **Index Pages**: When an item represents an index page, it calls gistScanPage to extract all items from that page and add them to the search queue.

3. **Distance Management**: For heap tuples, it stores the computed distances using index_store_float8_orderby_distances, which makes them available to the executor for ORDER BY operations.

4. **Memory Management**: Properly frees previously returned tuples and search items to prevent memory leaks.

The function continues processing until it finds a heap tuple or the queue is exhausted.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - getNextGISTSearchItem
  - GISTSearchItemIsHeap
  - index_store_float8_orderby_distances
  - gistScanPage
  - CHECK_FOR_INTERRUPTS
- Called from:
  - gistgettuple

## Notes and Other Information
- This is a static function only accessible within gistget.c
- Returns true when a heap tuple is found, false when the search queue is exhausted
- Critical for implementing nearest neighbor queries in GiST indexes
- Handles both regular ordered scans and index-only scans
- The function processes items in strict distance order due to the priority queue implementation
- Memory cleanup is performed for both the current xs_hitup and processed search items