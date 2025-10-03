# heapam_index_fetch_begin

## Location
[src/backend/access/heap/heapam_handler.c:80-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L80-L90)

## Overview
Initializes and returns a heap-specific index fetch data structure for beginning an index scan on a heap relation.

## Definition

```c
static IndexFetchTableData *
heapam_index_fetch_begin(Relation rel)
```
## Detailed Description
This function serves as the initialization callback for index fetch operations on heap tables within PostgreSQL's table access method framework. It allocates and initializes an IndexFetchHeapData structure, which extends the base IndexFetchTableData structure with heap-specific fields. The function sets up the necessary state for subsequent index fetch operations, including initializing the relation reference and setting the current buffer to invalid. This is part of the pluggable storage architecture that allows different table access methods to provide their own index fetch implementations.

## Parameters / Member Variables
- `rel`: The Relation object representing the heap table being scanned
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation function)
  - [IndexFetchHeapData](../I/IndexFetchHeapData.md) (heap-specific index fetch data structure)
  - InvalidBuffer (constant for invalid buffer state)
- Called from (representative examples):
  - Part of TableAmRoutine structure as a callback function
  - Referenced by SampleHeapTupleVisible

## Notes and Other Information
- Returns a pointer to the base IndexFetchTableData structure, allowing polymorphic usage
- Allocates memory using palloc0, ensuring zero-initialization
- Sets xs_cbuf to InvalidBuffer to indicate no buffer is currently pinned
- This is the first step in a sequence of index fetch operations (begin, fetch, reset, end)
- The allocated structure must be freed by calling the corresponding heapam_index_fetch_end function