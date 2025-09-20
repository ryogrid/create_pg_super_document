# SharedHashInfo

## Location
[src/include/nodes/execnodes.h:2734-2738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2734-L2738)

## Overview
SharedHashInfo is a structure that serves as a shared memory container for collecting hash instrumentation information from multiple parallel workers in PostgreSQL.

## Definition

```c
typedef struct SharedHashInfo
{
	int			num_workers;
	HashInstrumentation hinstrument[FLEXIBLE_ARRAY_MEMBER];
} SharedHashInfo;
```
## Detailed Description
SharedHashInfo facilitates the collection and aggregation of hash operation performance metrics across multiple parallel workers in PostgreSQL's parallel query execution system. It acts as a shared memory container that allows each parallel worker to record its hash instrumentation data, which can then be retrieved and aggregated by the main process for comprehensive performance analysis. The structure uses a flexible array member to accommodate a variable number of workers, with each worker having its own HashInstrumentation entry.

## Parameters / Member Variables
- `num_workers`: The number of parallel workers that will be recording hash instrumentation information
- `hinstrument`: Flexible array of HashInstrumentation structures, with one entry per worker for collecting individual worker hash performance metrics

## Dependencies
- Functions called/Symbols referenced:
  - [HashInstrumentation](../H/HashInstrumentation.md) (for individual worker metrics)
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array implementation)
- Called from (representative examples):
  - [show_hash_info](../s/show_hash_info.md) (for displaying aggregated hash information in EXPLAIN ANALYZE)
  - ExecHashEstimate (for estimating shared memory requirements)
  - ExecHashInitializeDSM (for initializing shared memory structures)
  - ExecHashInitializeWorker (for worker initialization)
  - ExecHashRetrieveInstrumentation (for collecting metrics from workers)

## Notes and Other Information
- Critical component of PostgreSQL's parallel hash join implementation
- Enables performance monitoring and analysis across parallel hash operations
- Uses shared memory to coordinate instrumentation data collection between processes
- The flexible array member allows the structure to scale with the number of parallel workers
- Essential for providing accurate EXPLAIN ANALYZE output for parallel hash operations
- Helps identify performance bottlenecks and resource usage patterns in parallel hash joins
- Part of the distributed instrumentation system that supports parallel query optimization
- Referenced by HashState structure for maintaining shared hash operation state
- Located in src/include/nodes/execnodes.h:2734-2738