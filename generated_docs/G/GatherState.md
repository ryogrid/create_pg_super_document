# GatherState

## Location
[src/include/nodes/execnodes.h:2669-2683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2669-L2683)

## Overview
GatherState is a structure that manages the execution state for PostgreSQL's GATHER node, which launches parallel workers to execute a subplan and collects the results from those workers.

## Definition

```c
typedef struct GatherState
{
	PlanState	ps;				/* its first field is NodeTag */
	bool		initialized;	/* workers launched? */
	bool		need_to_scan_locally;	/* need to read from local plan? */
	int64		tuples_needed;	/* tuple bound, see ExecSetTupleBound */
	/* these fields are set up once: */
	TupleTableSlot *funnel_slot;
	struct ParallelExecutorInfo *pei;
	/* all remaining fields are reinitialized during a rescan: */
	int			nworkers_launched;	/* original number of workers */
	int			nreaders;		/* number of still-active workers */
	int			nextreader;		/* next one to try to read from */
	struct TupleQueueReader **reader;	/* array with nreaders active entries */
} GatherState;
```
## Detailed Description
GatherState maintains the execution state for Gather nodes, which implement PostgreSQL's parallel query execution by launching one or more parallel workers to run a subplan and collecting the results. The structure manages both the parallel workers and the coordination required to gather results from multiple sources into a single stream. It handles worker lifecycle, tuple collection, and round-robin reading from active workers.

## Parameters / Member Variables
- `ps`: PlanState structure containing common executor node state information
- `initialized`: Boolean flag indicating whether parallel workers have been launched
- `need_to_scan_locally`: Boolean flag indicating whether the local plan also needs to be scanned (in addition to parallel workers)
- `tuples_needed`: Tuple bound limit for execution optimization (see ExecSetTupleBound)
- `funnel_slot`: TupleTableSlot used for collecting and funneling tuples from parallel workers
- `pei`: Pointer to ParallelExecutorInfo structure containing parallel execution context information
- `nworkers_launched`: Original number of parallel workers that were launched
- `nreaders`: Current number of still-active workers from which tuples can be read
- `nextreader`: Index of the next TupleQueueReader to attempt reading from (for round-robin scheduling)
- `reader`: Array of TupleQueueReader pointers with nreaders active entries for reading from parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited structure)
  - [TupleTableSlot](../T/TupleTableSlot.md) (for tuple collection)
  - [ParallelExecutorInfo](../P/ParallelExecutorInfo.md) (for parallel execution context)
  - [TupleQueueReader](../T/TupleQueueReader.md) (for reading from parallel worker queues)
- Called from (representative examples):
  - [ExecInitGather](../E/ExecInitGather.md) (initialization function)
  - [ExecGather](../E/ExecGather.md) (main execution function)
  - [ExecEndGather](../E/ExecEndGather.md) (cleanup function)
  - [gather_getnext](../g/gather_getnext.md) (tuple collection function)
  - [gather_readnext](../g/gather_readnext.md) (worker reading function)

## Notes and Other Information
- Essential component of PostgreSQL's parallel query execution system
- Manages the complex coordination between multiple parallel workers and the main process
- Uses round-robin scheduling via nextreader to fairly distribute read attempts across active workers
- Fields are strategically organized: some set up once during initialization, others reinitialized during rescans
- Works closely with TupleQueueReader for inter-process communication with parallel workers
- Located in src/include/nodes/execnodes.h:2669-2683