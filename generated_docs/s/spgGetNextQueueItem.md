# spgGetNextQueueItem

## Location
[src/backend/access/spgist/spgscan.c:746-754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L746-L754)

## Overview
Retrieves the next item from an ordered scan queue during SP-GiST index scanning, returning NULL when the index is exhausted.

## Definition

```c
enum SpGistSpecialOffsetNumbers
{
	SpGistBreakOffsetNumber = InvalidOffsetNumber,
	SpGistRedirectOffsetNumber = MaxOffsetNumber + 1,
	SpGistErrorOffsetNumber = MaxOffsetNumber + 2,
};
```
## Detailed Description
This function serves as a queue management utility for SP-GiST (Space-partitioned Generalized Search Tree) index scans. It operates on a priority queue (pairing heap) that maintains search items in order during index traversal. The function performs a simple but critical role: it checks if the scan queue is empty and either returns the next item to process or signals completion of the scan by returning NULL. The caller is responsible for freeing the returned item.

## Parameters / Member Variables
- : SpGistScanOpaque structure containing the scan state, including the scanQueue (pairing heap) that stores search items in priority order

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - [pairingheap_remove_first](../p/pairingheap_remove_first.md)
  - SpGistScanOpaque
  - [SpGistSearchItem](../S/SpGistSearchItem.md)
- Called from (representative examples):
  - [spgWalk](spgWalk.md)

## Notes and Other Information
- This is a static function internal to spgscan.c
- The function assumes the caller will properly free the returned SpGistSearchItem
- The pairing heap implementation ensures items are returned in the correct order for the scan
- Located at src/backend/access/spgist/spgscan.c:746-754

## Simplified Source

```c
static SpGistSearchItem *
spgGetNextQueueItem(SpGistScanOpaque so)
{
    // Check if scan queue is empty
    if (pairingheap_is_empty(so->scanQueue))
        return NULL;  // Scan is complete

    // Remove and return next item from priority queue
    return (SpGistSearchItem *) pairingheap_remove_first(so->scanQueue);
}
```