# _bt_dedup_start_pending

## Location
[src/backend/access/nbtree/nbtdedup.c:433-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L433-L483)

## Overview
Initializes a new pending posting list tuple based on a base tuple, setting up the deduplication state for collecting duplicate tuples.

## Definition

```c
void
_bt_dedup_start_pending(BTDedupState state, IndexTuple base,
						OffsetNumber baseoff)
```
## Detailed Description
This function serves as the initialization step for creating a new posting list during deduplication. Every tuple processed during deduplication either becomes the base tuple for a posting list or gets its heap TIDs merged into an existing pending posting list.

The function handles two types of base tuples:
1. **Regular tuples**: Single heap TID copied directly
2. **Existing posting list tuples**: Multiple heap TIDs copied from the existing posting list

The function sets up all necessary state information including the base tuple reference, heap TID array, size calculations, and interval tracking. The base tuple will only be rewritten if duplicates are found and merged during the deduplication process.

## Parameters / Member Variables
- : Deduplication state structure containing working arrays and metadata
- : Index tuple that will serve as the base for the new pending posting list
- : Offset number of the base tuple on the page

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if tuple is a pivot tuple
  - : Determines if tuple is already a posting list
  - : Gets number of heap TIDs in existing posting list
  - : Extracts heap TID array from posting list tuple
  - : Gets offset where posting list data begins
  - : Calculates total size of index tuple

- Called from (representative examples):
  - : Called for first tuple and when starting new intervals
  - : Called when starting new duplicate intervals
  - : Called during index build operations
  - : Called during WAL replay of deduplication

## Notes and Other Information
- The function asserts that no pending state exists (nhtids == 0, nitems == 0)
- Only works with leaf tuples, not pivot tuples (enforced by assertion)
- For existing posting lists, basetupsize excludes the posting list data to calculate space for a new posting list
- Physical size calculation includes MAXALIGN overhead and line pointer size for accurate space accounting
- The baseoff is saved in the intervals array to track the original location of the base tuple
- This function is always paired with subsequent calls to  and 