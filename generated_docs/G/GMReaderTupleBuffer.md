# GMReaderTupleBuffer

## Location
[src/backend/executor/nodeGatherMerge.c:41-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L41-L47)

## Overview
GMReaderTupleBuffer is a struct that manages pending tuples fetched from worker processes in PostgreSQL's Gather Merge parallel query execution, providing efficient buffering and tracking of tuple processing state for each worker.

## Definition

```c
typedef struct GMReaderTupleBuffer
{
	MinimalTuple *tuple;		/* array of length MAX_TUPLE_STORE */
	int			nTuples;		/* number of tuples currently stored */
	int			readCounter;	/* index of next tuple to extract */
	bool		done;			/* true if reader is known exhausted */
} GMReaderTupleBuffer;
```
## Detailed Description
The GMReaderTupleBuffer struct is used in PostgreSQL's Gather Merge node implementation to buffer tuples from worker processes during parallel query execution. This structure serves as a pending-tuple array for each worker, holding additional tuples that have been fetched from the worker but cannot be processed immediately due to the merge ordering requirements.

The buffer implements a simple array-based queue system where tuples are stored in the  array and accessed sequentially using the  index. This design minimizes context-switching overhead by reading multiple tuples at once from workers while managing memory usage by limiting the buffer size to MAX_TUPLE_STORE (10) tuples.

The struct is specifically designed for worker processes only - it is not used for the leader process, which doesn't keep pending tuples and uses the  flag as its completion indicator.

## Parameters / Member Variables
- : Array of MinimalTuple pointers with a fixed length of MAX_TUPLE_STORE (10), storing the buffered tuples from a worker
- : Integer count of tuples currently stored in the buffer
- : Index pointing to the next tuple to be extracted from the buffer during processing
- : Boolean flag indicating whether the associated worker is known to have no more tuples to provide

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (tuple storage type)
  - MAX_TUPLE_STORE (buffer size constant)
  
- Called from (representative examples):
  - [gather_merge_setup](../g/gather_merge_setup.md) (initialization)
  - [gather_merge_clear_tuples](../g/gather_merge_clear_tuples.md) (cleanup)
  - [load_tuple_array](../l/load_tuple_array.md) (buffer loading)
  - [gather_merge_readnext](../g/gather_merge_readnext.md) (tuple retrieval)
  - [GatherMergeState](GatherMergeState.md) (parent execution state structure)

## Notes and Other Information
- The buffer size is intentionally limited to MAX_TUPLE_STORE (10) tuples to balance performance gains from batch reading against memory consumption
- This structure is only used for worker processes in parallel execution; the leader process uses different tracking mechanisms
- The  and  work together to implement a simple queue: tuples are added at  and consumed starting from 
- The  flag provides early termination detection to avoid unnecessary polling of exhausted workers
- Located in src/backend/executor/nodeGatherMerge.c:41-47