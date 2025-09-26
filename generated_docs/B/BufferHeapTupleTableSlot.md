# BufferHeapTupleTableSlot

## Location
[src/include/executor/tuptable.h:267-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L267-L280)

## Overview
BufferHeapTupleTableSlot is a specialized table slot structure for heap tuples that reside in shared buffer pages, extending HeapTupleTableSlot to manage buffer pin relationships.

## Definition

```c
typedef struct BufferHeapTupleTableSlot
{
	pg_node_attr(abstract)

	HeapTupleTableSlot base;

	/*
	 * If buffer is not InvalidBuffer, then the slot is holding a pin on the
	 * indicated buffer page; drop the pin when we release the slot's
	 * reference to that buffer.  (TTS_FLAG_SHOULDFREE should not be set in
	 * such a case, since presumably base.tuple is pointing into the buffer.)
	 */
	Buffer		buffer;			/* tuple's buffer, or InvalidBuffer */
} BufferHeapTupleTableSlot;
```
## Detailed Description
BufferHeapTupleTableSlot is a specialized tuple table slot implementation designed to handle heap tuples that reside directly in shared buffer pages. This structure extends HeapTupleTableSlot by adding buffer management capabilities, specifically maintaining a pin on the buffer page containing the tuple data.

The key design principle is that when a tuple is stored in a buffer page, the slot must maintain a pin on that buffer to prevent the page from being evicted from the buffer pool while the tuple is being accessed. This ensures data consistency and prevents the tuple pointer from becoming invalid due to buffer replacement.

When the buffer field is set to a valid buffer ID (not InvalidBuffer), the slot holds a pin on that buffer page, and the tuple data pointed to by base.tuple is expected to reside within that buffer page. In this case, the TTS_FLAG_SHOULDFREE flag should not be set since the tuple data is not separately allocated but rather points into the buffer page.

## Parameters / Member Variables
- `base`: HeapTupleTableSlot structure containing the basic tuple slot functionality and tuple pointer
- `buffer`: Buffer identifier for the buffer page containing the tuple data, or InvalidBuffer if the tuple is not in a buffer
## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleTableSlot](../H/HeapTupleTableSlot.md)
  - Buffer (type)
  - InvalidBuffer (constant)

- Called from (representative examples):
  - [heapam_index_fetch_tuple](../h/heapam_index_fetch_tuple.md)
  - [heapam_fetch_row_version](../h/heapam_fetch_row_version.md)  
  - [heapam_tuple_satisfies_snapshot](../h/heapam_tuple_satisfies_snapshot.md)
  - [tts_buffer_heap_clear](../t/tts_buffer_heap_clear.md)
  - [tts_buffer_heap_materialize](../t/tts_buffer_heap_materialize.md)
  - [ExecForceStoreHeapTuple](../E/ExecForceStoreHeapTuple.md)

## Notes and Other Information
- This slot type is primarily used by the heap access method handler functions
- Buffer pin management is critical - the pin must be properly released when the slot is cleared or destroyed
- The buffer field being InvalidBuffer indicates the tuple is not currently associated with a buffer page
- Used extensively in heap table operations including scanning, fetching, and tuple manipulation
- Part of PostgreSQL's tuple table slot framework which provides a unified interface for different tuple storage formats