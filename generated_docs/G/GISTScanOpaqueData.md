# GISTScanOpaqueData

## Location
src/include/access/gist_private.h: 154 - 179

## Overview
GISTScanOpaqueData maintains the complete private state for a GiST index scan operation, including the search queue, workspace areas, and buffers for efficient tuple retrieval.

## Definition


## Detailed Description
GISTScanOpaqueData serves as the comprehensive state holder for GiST index scan operations, encapsulating all necessary information for both ordered and non-ordered searches. The structure manages the core search infrastructure including the pairing heap-based priority queue for unvisited items, workspace areas for distance calculations, and specialized buffers for efficient tuple retrieval during non-ordered scans.

The design accommodates both distance-ordered searches (using ORDER BY clauses) and regular searches, with different optimization strategies for each. For non-ordered searches, the pageData array acts as a local buffer to collect all returnable items from a page before processing, improving efficiency by reducing queue operations. The structure also includes facilities for tracking killed items and managing memory contexts for different aspects of the scan operation.

## Parameters / Member Variables
- : Pointer to GISTSTATE containing all index-specific information and cached support functions
- : Array of Oid values representing the datatypes of ORDER BY expressions in ordered searches
- : Pairing heap managing the queue of unvisited GISTSearchItem entries
- : Memory context that holds the search queue and related data structures
- : Boolean flag indicating whether the search qualifiers can ever be satisfied (false means early termination)
- : Boolean flag tracking whether this is the first call to gistgettuple for initialization purposes
- : Pre-allocated workspace array for storing distance calculation results from gistindex_keytest
- : Array of offset numbers for items that have been marked as killed/dead
- : Current count of items stored in the killedItems array
- : Block number of the page currently being processed
- : LSN (Log Sequence Number) position in the WAL stream when the current page was read
- : Fixed-size array storing returnable heap items for non-ordered searches
- : Number of valid entries currently stored in the pageData array
- : Index of the next item to return from the pageData array
- : Memory context holding fetched tuples specifically for index-only scan operations

## Dependencies
- Functions called/Symbols referenced:
  - GISTSTATE
  - pairingheap
  - IndexOrderByDistance
  - GistNSN
  - GISTSearchHeapItem
  - IndexTupleData
  - MemoryContext
  - Oid
  - OffsetNumber
  - BlockNumber
- Called from (representative examples):
  - gistbeginscan
  - GISTScanOpaque (typedef)

## Notes and Other Information
This structure is typically allocated and initialized during gistbeginscan() and persists throughout the entire scan operation. The pageData array optimization is particularly important for non-ordered scans, as it allows batching of returnable items from each page rather than processing them individually through the queue system. The dual memory context design (queueCxt and pageDataCxt) enables fine-grained memory management for different aspects of the scan. The killed items tracking mechanism supports PostgreSQL's tuple visibility and cleanup operations during concurrent access scenarios.