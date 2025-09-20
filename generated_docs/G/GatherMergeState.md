# GatherMergeState

## Location
[src/include/nodes/execnodes.h:2695-2715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2695-L2715)

## Overview
GatherMergeState is a structure that manages the execution state for PostgreSQL's GATHER MERGE node, which launches parallel workers to execute a subplan that produces sorted output and merges the results into a single sorted stream.

## Definition

```c
typedef struct GatherMergeState
{
	PlanState	ps;				/* its first field is NodeTag */
	bool		initialized;	/* workers launched? */
	bool		gm_initialized; /* gather_merge_init() done? */
	bool		need_to_scan_locally;	/* need to read from local plan? */
	int64		tuples_needed;	/* tuple bound, see ExecSetTupleBound */
	/* these fields are set up once: */
	TupleDesc	tupDesc;		/* descriptor for subplan result tuples */
	int			gm_nkeys;		/* number of sort columns */
	SortSupport gm_sortkeys;	/* array of length gm_nkeys */
	struct ParallelExecutorInfo *pei;
	/* all remaining fields are reinitialized during a rescan */
	/* (but the arrays are not reallocated, just cleared) */
	int			nworkers_launched;	/* original number of workers */
	int			nreaders;		/* number of active workers */
	TupleTableSlot **gm_slots;	/* array with nreaders+1 entries */
	struct TupleQueueReader **reader;	/* array with nreaders active entries */
	struct GMReaderTupleBuffer *gm_tuple_buffers;	/* nreaders tuple buffers */
	struct binaryheap *gm_heap; /* binary heap of slot indices */
} GatherMergeState;
```
## Detailed Description
GatherMergeState extends the concept of parallel execution beyond simple gathering to include merge functionality. It launches multiple parallel workers that each produce sorted output according to specified sort keys, then uses a binary heap-based merge algorithm to combine these sorted streams into a single globally sorted result stream. This enables efficient parallel execution of operations that require sorted output, such as ORDER BY clauses and merge joins.

## Parameters / Member Variables
- `ps`: PlanState structure containing common executor node state information
- `initialized`: Boolean flag indicating whether parallel workers have been launched
- `gm_initialized`: Boolean flag indicating whether gather_merge_init() initialization has been completed
- `need_to_scan_locally`: Boolean flag indicating whether the local plan also needs to be scanned alongside parallel workers
- `tuples_needed`: Tuple bound limit for execution optimization (see ExecSetTupleBound)
- `tupDesc`: TupleDesc describing the structure of result tuples from the subplan
- `gm_nkeys`: Number of sort columns used for merging
- `gm_sortkeys`: Array of SortSupport structures defining the sort criteria for each column
- `pei`: Pointer to ParallelExecutorInfo structure containing parallel execution context
- `nworkers_launched`: Original number of parallel workers that were launched
- `nreaders`: Current number of active workers from which sorted data can be read
- `gm_slots`: Array of TupleTableSlot pointers with nreaders+1 entries for holding tuples during merge
- `reader`: Array of TupleQueueReader pointers for reading from parallel worker queues
- `gm_tuple_buffers`: Array of GMReaderTupleBuffer structures for buffering tuples from each reader
- `gm_heap`: Binary heap structure used to efficiently determine which tuple should be output next during merge

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited structure)
  - [TupleDesc](../T/TupleDesc.md) (for tuple structure description)
  - SortSupport (for sort key management)
  - ParallelExecutorInfo (for parallel execution context)
  - TupleTableSlot (for tuple storage during merge)
  - [TupleQueueReader](../T/TupleQueueReader.md) (for reading from parallel workers)
  - [GMReaderTupleBuffer](GMReaderTupleBuffer.md) (for tuple buffering)
  - [binaryheap](../b/binaryheap.md) (for efficient merge ordering)
- Called from (representative examples):
  - [ExecInitGatherMerge](../E/ExecInitGatherMerge.md) (initialization function)
  - [ExecGatherMerge](../E/ExecGatherMerge.md) (main execution function)
  - [ExecEndGatherMerge](../E/ExecEndGatherMerge.md) (cleanup function)
  - [gather_merge_getnext](../g/gather_merge_getnext.md) (tuple retrieval function)
  - [gather_merge_setup](../g/gather_merge_setup.md) (setup function)

## Notes and Other Information
- Implements sophisticated merge-sort algorithm for parallel query results
- Uses binary heap for O(log n) complexity when selecting next tuple from multiple sorted streams
- Critical for enabling parallel execution while maintaining sorted output order
- More complex than GatherState due to the need to maintain sort order across parallel streams
- Arrays are not reallocated during rescans for efficiency, only cleared and reused
- The +1 in gm_slots accounts for the local process in addition to parallel workers
- [GMReaderTupleBuffer](GMReaderTupleBuffer.md) is defined privately in nodeGatherMerge.c for encapsulation
- Located in src/include/nodes/execnodes.h:2695-2715