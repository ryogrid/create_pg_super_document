# TidStoreIter

## Location
[src/backend/access/common/tidstore.c:135-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L135-L164)

## Overview
TidStoreIter is an iterator structure that provides sequential access to tuple identifiers (TIDs) stored in a TidStore, supporting both local and shared memory iteration modes.

## Definition

```c
struct TidStoreIter
{
	TidStore   *ts;

	/* iterator of radix tree. Use either one depending on TidStoreIsShared() */
	union
	{
		shared_ts_iter *shared;
		local_ts_iter *local;
	}			tree_iter;

	/* output for the caller */
	TidStoreIterResult output;
};
```
## Detailed Description
TidStoreIter provides a standardized interface for iterating through all tuple identifiers stored in a TidStore. The iterator abstracts away the underlying storage details (local vs shared memory) and presents a uniform API for sequential access to TIDs. It maintains state for the current iteration position within the radix tree structure and provides results through the TidStoreIterResult output structure. The iterator is designed to handle both sparse and dense TID distributions efficiently, working in conjunction with the BlocktableEntry structures to extract TIDs from their optimized storage formats. This design enables efficient traversal of large TID collections while maintaining good performance characteristics regardless of the underlying storage mode.

## Parameters / Member Variables
- : Pointer to the TidStore being iterated over
- : Iterator for shared memory radix tree when operating in multi-backend mode
- : Iterator for local memory radix tree when operating in single-backend mode  
- : TidStoreIterResult structure containing the current iteration results for the caller

## Dependencies
- Functions called/Symbols referenced:
  - [TidStore](TidStore.md) (parent structure type)
  - TidStoreIterResult (output structure type)
  - [BlocktableEntry](../B/BlocktableEntry.md) (for TID extraction)
  - [tidstore_iter_extract_tids](../t/tidstore_iter_extract_tids.md) (helper function)
- Called from (representative examples):
  - [TidStoreBeginIterate](TidStoreBeginIterate.md)
  - [TidStoreIterateNext](TidStoreIterateNext.md)
  - [TidStoreEndIterate](TidStoreEndIterate.md)
  - [lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)

## Notes and Other Information
- The union design allows efficient switching between local and shared iteration modes without code duplication
- Works closely with tidstore_iter_extract_tids helper function to convert BlocktableEntry data into accessible TID arrays
- Maintains iteration state internally, requiring proper initialization via TidStoreBeginIterate and cleanup via TidStoreEndIterate
- Used extensively in vacuum operations to sequentially process all dead tuple identifiers
- The iterator design abstracts complexity of the underlying radix tree traversal from callers
- Supports efficient iteration over large TID collections with minimal memory overhead