# gm_readnext_tuple

## Location
src/backend/executor/nodeGatherMerge.c: 707 - 738

## Overview
Reads the next tuple from a specific parallel worker's tuple queue and creates a copy of it for buffering in the Gather Merge operation.

## Definition
```c
static MinimalTuple gm_readnext_tuple(GatherMergeState *gm_state, int nreader, bool nowait, bool *done)
```

## Detailed Description
This function serves as a low-level interface for reading tuples from parallel workers in PostgreSQL's Gather Merge execution. It acts as a wrapper around the TupleQueueReaderNext function, providing additional functionality specific to the Gather Merge context.

The function handles the underlying communication with parallel worker processes through shared memory tuple queues. It includes important safety mechanisms such as interrupt checking and graceful handling of worker initialization failures. When a worker fails to initialize, TupleQueueReaderNext returns NULL, which this function handles by treating the worker as having produced no tuples.

A critical aspect of this function is that it creates a copy of any successfully read tuple using heap_copy_minimal_tuple(). This copying is essential because tuples will be buffered across multiple function calls, and the original tuple memory from the shared queue may be reused or freed by the time the buffered tuple is accessed.

## Parameters / Member Variables
- `gm_state`: Pointer to the GatherMergeState structure containing the overall parallel merge execution state and worker readers
- `nreader`: Integer identifier for the worker process (1-based indexing, where 1 represents the first worker)
- `nowait`: Boolean flag indicating whether to block waiting for a tuple (false) or return immediately if none available (true)
- `done`: Output parameter (boolean pointer) that is set to true if the specified worker has finished producing tuples

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [TupleQueueReaderNext](../T/TupleQueueReaderNext.md)
  - [heap_copy_minimal_tuple](../h/heap_copy_minimal_tuple.md)
  - [GatherMergeState](../G/GatherMergeState.md)
  - [TupleQueueReader](../T/TupleQueueReader.md)
  - MinimalTuple
- Called from (representative examples):
  - [load_tuple_array](../l/load_tuple_array.md)
  - [gather_merge_readnext](gather_merge_readnext.md)

## Notes and Other Information
- Returns a MinimalTuple if successful, NULL if no tuple is available or worker is done
- Always creates a copy of the tuple to ensure safe buffering and memory management
- Handles worker initialization failures gracefully by returning NULL
- Uses 1-based indexing for nreader parameter (worker 1 is at index 0 in the reader array)
- Includes interrupt checking to allow query cancellation during tuple reads
- Part of the parallel query execution infrastructure that enables efficient tuple streaming from multiple workers
- The function is designed to work with PostgreSQL's shared memory message queue system for parallel processing