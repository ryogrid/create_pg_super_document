# Gather

## Location
[src/include/nodes/plannodes.h:1140-1149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1140-L1149)

## Overview
The Gather node implements parallel query execution by launching multiple worker processes to execute a subplan and collecting the results from all workers into a single stream.

## Definition

```c
typedef struct Gather
{
	Plan		plan;
	int			num_workers;	/* planned number of worker processes */
	int			rescan_param;	/* ID of Param that signals a rescan, or -1 */
	bool		single_copy;	/* don't execute plan more than once */
	bool		invisible;		/* suppress EXPLAIN display (for testing)? */
	Bitmapset  *initParam;		/* param id's of initplans which are referred
								 * at gather or one of it's child node */
} Gather;
```
## Detailed Description
The Gather node is the coordinator for parallel query execution in PostgreSQL. It launches multiple worker processes (backends) that each execute the same subplan independently on different portions of data. The Gather node then collects results from all workers via tuple queues and merges them into a single output stream.

The node supports both parallel-aware and parallel-oblivious execution modes:
- Parallel-aware: The subplan is designed to work with parallelism (e.g., parallel sequential scans)
- Parallel-oblivious: Regular plans that can be safely executed in parallel workers

Key operational aspects:
- Workers are launched on first execution, not during initialization
- The leader process can also participate in executing the plan locally
- Results are collected through tuple queues that workers write to
- The node handles worker lifecycle management and cleanup

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : The planned number of parallel worker processes to launch
- : Parameter ID used to signal rescans to parallel-aware child nodes (-1 if not needed)
- : When true, prevents executing the plan more than once (used for certain operations that should not be duplicated)
- : Flag to suppress display in EXPLAIN output (primarily for testing purposes)
- : Bitmap set of parameter IDs for InitPlans referenced by this node or its children

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - [Bitmapset](../B/Bitmapset.md)
  - [ParallelExecutorInfo](../P/ParallelExecutorInfo.md)
  - [TupleQueueReader](../T/TupleQueueReader.md)
- Called from (representative examples):
  - [ExecInitGather](../E/ExecInitGather.md)
  - [ExecGather](../E/ExecGather.md)
  - [ExecReScanGather](../E/ExecReScanGather.md)
  - [create_gather_plan](../c/create_gather_plan.md)
  - [make_gather](../m/make_gather.md)

## Notes and Other Information
- The Gather node is essential for PostgreSQL's parallel query execution framework
- Worker processes are separate backend processes, not threads
- The number of workers actually launched may be less than requested due to system limitations
- The rescan_param mechanism ensures that parallel-aware scan nodes properly handle rescans
- Tuple queues provide the communication mechanism between workers and the leader
- The node can fall back to serial execution if no workers can be launched
- Memory management is carefully handled to avoid leaks across parallel boundaries