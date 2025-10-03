# spgAddSearchItemToQueue

## Location
[src/backend/access/spgist/spgscan.c:108-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L108-L113)

## Overview
A simple wrapper function that adds a SpGistSearchItem to the priority queue used in SP-GiST scan operations.

## Definition

```c
static void
spgAddSearchItemToQueue(SpGistScanOpaque so, SpGistSearchItem *item)
```
## Detailed Description
This function serves as a straightforward interface for adding search items to the pairing heap-based priority queue used in SP-GiST (Space-Partitioned Generalized Search Tree) scans. It encapsulates the pairing heap insertion operation and maintains the abstraction between the search algorithm and the underlying queue data structure.

The function is designed to be called in queue context, meaning it operates on items that are ready to be processed as part of the search traversal. The pairing heap ensures that items are processed in the correct priority order as determined by the comparison function (pairingheap_SpGistSearchItem_cmp).

## Parameters / Member Variables
- `so`: SpGistScanOpaque structure containing the scan context, including the scanQueue pairing heap
- `*item`: Pointer to the SpGistSearchItem to be added to the queue for future processing
## Dependencies
- Functions called/Symbols referenced:
  - [pairingheap_add](../p/pairingheap_add.md) (adds node to pairing heap data structure)
  - SpGistScanOpaque (scan operation context structure)
  - [SpGistSearchItem](../S/SpGistSearchItem.md) (search item structure containing phNode member)
- Called from (representative examples):
  - [spgAddStartItem](spgAddStartItem.md) (adds initial search items to queue)
  - [spgLeafTest](spgLeafTest.md) (adds leaf items during search traversal)
  - [spgInnerTest](spgInnerTest.md) (adds inner node items during search traversal)

## Notes and Other Information
- Simple one-line wrapper function that provides abstraction over pairing heap operations
- Uses the phNode member of SpGistSearchItem as the pairing heap node
- Essential component of the search queue management in SP-GiST scans
- The queue ordering is determined by the pairingheap_SpGistSearchItem_cmp comparison function
- Called exclusively in contexts where items are ready for queue insertion