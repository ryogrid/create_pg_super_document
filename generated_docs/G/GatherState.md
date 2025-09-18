# GatherState

## Location
src/include/nodes/execnodes.h: 2669 - 2683

## Overview
GatherState is a structure that manages the execution state for PostgreSQL's GATHER node, which launches parallel workers to execute a subplan and collects the results from those workers.

## Definition


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
  - PlanState (inherited structure)
  - TupleTableSlot (for tuple collection)
  - ParallelExecutorInfo (for parallel execution context)
  - TupleQueueReader (for reading from parallel worker queues)
- Called from (representative examples):
  - ExecInitGather (initialization function)
  - ExecGather (main execution function)
  - ExecEndGather (cleanup function)
  - gather_getnext (tuple collection function)
  - gather_readnext (worker reading function)

## Notes and Other Information
- Essential component of PostgreSQL's parallel query execution system
- Manages the complex coordination between multiple parallel workers and the main process
- Uses round-robin scheduling via nextreader to fairly distribute read attempts across active workers
- Fields are strategically organized: some set up once during initialization, others reinitialized during rescans
- Works closely with TupleQueueReader for inter-process communication with parallel workers
- Located in src/include/nodes/execnodes.h:2669-2683