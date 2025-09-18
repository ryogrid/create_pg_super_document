# getNextGISTSearchItem

## Location
[src/backend/access/gist/gistget.c:538-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L538-L559)

## Overview
getNextGISTSearchItem extracts the next item in order from the GiST search queue, which is implemented as a priority heap for ordered scans.

## Definition
static GISTSearchItem *getNextGISTSearchItem(GISTScanOpaque so)

## Detailed Description
This function serves as the primary interface for retrieving the next search item from the GiST scan queue. The queue is implemented using a pairing heap data structure that maintains items in order based on their distances (for ordered scans) or insertion order (for unordered scans).

The function simply checks if the queue is empty and either:
1. Removes and returns the first item from the pairing heap if items exist
2. Returns NULL if the queue is empty, indicating scan completion

The caller is responsible for freeing the returned GISTSearchItem using pfree() when done processing it.

## Parameters / Member Variables
- `so`: GISTScanOpaque containing the scan state, including the priority queue

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - pairingheap_remove_first
- Called from:
  - [getNextNearest](getNextNearest.md)
  - [gistgettuple](gistgettuple.md)
  - [gistgetbitmap](gistgetbitmap.md)

## Notes and Other Information
- This is a static function only accessible within gistget.c
- Returns NULL when the search queue is exhausted, signaling scan completion
- The pairing heap ensures that items are returned in the correct order for ordered scans
- Caller must handle memory management by calling pfree() on returned items
- Simple wrapper around pairing heap operations for cleaner code organization