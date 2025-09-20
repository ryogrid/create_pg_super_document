# heap_hot_search_buffer

## Location
[src/backend/access/heap/heapam.c:1675-1826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1675-L1826)

## Overview
This function searches through a HOT (Heap-Only Tuples) chain to find the first tuple that satisfies a given snapshot's visibility requirements, optionally tracking whether all chain members are globally dead.

## Definition

```c
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call)
```
## Detailed Description
heap_hot_search_buffer traverses a HOT chain starting from a given TID to find a tuple visible to the specified snapshot. HOT chains are created when updates don't change indexed columns, allowing multiple tuple versions to share the same index entry. The function follows the chain by examining each tuple's t_ctid pointer, validating chain integrity through xmin/xmax relationships, and testing visibility.

The function handles both initial calls and continuation calls (when first_call is false), ensuring it doesn't return the same tuple repeatedly. It optionally tracks global deadness of all chain members for vacuum planning purposes and performs proper serializable isolation checks.

## Parameters / Member Variables
- : Input/output TID pointer; updated to point to the visible tuple if found
- : The heap relation containing the HOT chain
- : Pinned and locked buffer containing the page with the HOT chain
- : Snapshot for visibility testing
- : Caller-provided buffer filled with tuple data when a match is found
- : Optional output flag indicating if all chain members are globally dead
- : If true, this is the initial search; if false, skip the first tuple found

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md): Get page from buffer
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber: Extract TID components
  - [PageGetItemId](../P/PageGetItemId.md)/PageGetItem: Access page-level tuple data
  - ItemIdIsRedirected/ItemIdGetRedirect: Handle redirected item pointers
  - HeapTupleIsHeapOnly: Check if tuple is heap-only
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md): Test tuple visibility against snapshot
  - [PredicateLockTID](../P/PredicateLockTID.md): Acquire predicate locks for serializable isolation
  - [GlobalVisTestFor](../G/GlobalVisTestFor.md)/HeapTupleIsSurelyDead: Test global tuple deadness
  - HeapTupleIsHotUpdated: Check if tuple continues the HOT chain
- Called from (representative examples):
  - [heapam_index_fetch_tuple](heapam_index_fetch_tuple.md): Index access method tuple fetching
  - [heapam_scan_bitmap_next_block](heapam_scan_bitmap_next_block.md): Bitmap scan implementation
  - [heap_index_delete_tuples](heap_index_delete_tuples.md): Index tuple deletion

## Notes and Other Information
- Requires caller to already have the buffer pinned and locked (unlike heap_fetch)
- Maintains HOT chain integrity by validating xmin/xmax transaction relationships
- Handles redirected item pointers that can appear at chain start
- The first_call/skip mechanism prevents returning the same tuple on continuation calls
- Global deadness tracking helps vacuum identify cleanup opportunities
- Performs serializable conflict detection and predicate locking for proper isolation
- Chain traversal stops at non-HOT-updated tuples or broken transaction relationships