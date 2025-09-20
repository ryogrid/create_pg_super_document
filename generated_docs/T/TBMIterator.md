# TBMIterator

## Location
[src/backend/nodes/tidbitmap.c:178-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L178-L190)

## Overview
TBMIterator is a stateful iterator structure that enables sorted traversal of TIDBitmap contents, supporting concurrent iteration by multiple processes while maintaining read-only access to the underlying bitmap data.

## Definition

```c
struct TBMIterator
{
	TIDBitmap  *tbm;			/* TIDBitmap we're iterating over */
	int			spageptr;		/* next spages index */
	int			schunkptr;		/* next schunks index */
	int			schunkbit;		/* next bit to check in current schunk */
	TBMIterateResult output;	/* MUST BE LAST (because variable-size) */
};
```
## Detailed Description
TBMIterator provides a mechanism for sequential, sorted access to the tuple identifiers stored in a TIDBitmap. The iterator maintains separate pointers for exact pages (spageptr) and lossy chunks (schunkptr), allowing it to traverse both types of entries in block number order. 

The design supports multiple concurrent iterators on the same bitmap, which is essential for parallel query execution. Once any iterator is created, the underlying TIDBitmap becomes read-only to ensure consistency across all iterators. The variable-sized output field stores the results of each iteration step, accommodating different numbers of tuple identifiers per page.

The iterator's state tracking enables efficient resumption of traversal across multiple calls, making it suitable for integration with PostgreSQL's executor framework where scanning may be interrupted and resumed based on query execution flow.

## Parameters / Member Variables
- : Pointer to the TIDBitmap being iterated over, providing access to the sorted page and chunk arrays
- : Index pointer into the sorted exact pages array (spages), tracking progress through precise page entries
- : Index pointer into the sorted lossy chunks array (schunks), tracking progress through lossy chunk entries
- : Bit position tracker within the current lossy chunk, indicating which page within the chunk to examine next
- : Variable-sized result structure containing the tuple identifiers returned by the current iteration step, positioned last to accommodate flexible array sizing

## Dependencies
- Functions called/Symbols referenced:
  - [TIDBitmap](TIDBitmap.md)
  - TBMIterateResult
- Called from (representative examples):
  - [BitmapHeapNext](../B/BitmapHeapNext.md)
  - [BitmapAdjustPrefetchIterator](../B/BitmapAdjustPrefetchIterator.md)
  - [BitmapPrefetch](../B/BitmapPrefetch.md)
  - [tbm_begin_iterate](../t/tbm_begin_iterate.md)
  - [tbm_iterate](../t/tbm_iterate.md)
  - [tbm_end_iterate](../t/tbm_end_iterate.md)

## Notes and Other Information
- The read-only constraint on the TIDBitmap once iteration begins is crucial for maintaining consistency in parallel execution environments
- The dual pointer system (spageptr, schunkptr) enables efficient merging of exact and lossy entries during sorted traversal
- The variable-sized output field design optimizes memory usage by accommodating the actual number of tuples found per page
- [TBMIterator](TBMIterator.md) is integral to PostgreSQL's bitmap heap scan execution, providing the interface between bitmap index results and heap tuple retrieval
- The iterator state can be used for prefetching optimizations in bitmap heap scans, improving I/O performance
- Multiple iterators can coexist on the same bitmap, enabling sophisticated parallel scan strategies