# ExecHashEstimate

## Location
[src/backend/executor/nodeHash.c:2741-2759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2741-L2759)

## Overview
Reserves space in the Dynamic Shared Memory (DSM) segment for hash join instrumentation data when executing parallel hash joins.

## Definition

```c
void
ExecHashEstimate(HashState *node, ParallelContext *pcxt)
```
## Detailed Description
ExecHashEstimate is responsible for calculating and reserving the required amount of shared memory space for hash join instrumentation data in parallel query execution. This function is part of PostgreSQL's parallel query infrastructure and ensures that sufficient shared memory is allocated to collect performance statistics from all worker processes.

The function performs early validation checks to determine if instrumentation is actually needed. It only proceeds with memory estimation if instrumentation is enabled on the hash node and there are worker processes that will be participating in the parallel execution.

When instrumentation is required, the function calculates the total memory needed based on the number of worker processes. Each worker needs space for a HashInstrumentation structure, and the calculation includes the overhead for the SharedHashInfo container structure. The memory estimation is then registered with the shared memory table-of-contents (TOC) system for proper allocation during parallel query setup.

## Parameters / Member Variables
- : HashState containing the hash node configuration and instrumentation settings
- : ParallelContext containing worker count and memory estimation infrastructure

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md) (safely multiplies sizes to avoid overflow)
  - [add_size](../a/add_size.md) (safely adds sizes to avoid overflow)
  - shm_toc_estimate_chunk (estimates shared memory chunk requirements)
  - shm_toc_estimate_keys (estimates shared memory key requirements)
  - [HashInstrumentation](../H/HashInstrumentation.md) (structure for collecting hash join performance data)
  - [SharedHashInfo](../S/SharedHashInfo.md) (container structure for shared hash instrumentation data)
- Called from:
  - [ExecParallelEstimate](ExecParallelEstimate.md) (during parallel query planning phase)
  - Referenced in nodeHash.h header

## Notes and Other Information
- Only performs memory estimation when both instrumentation is enabled and workers are present
- Uses safe arithmetic functions (mul_size, add_size) to prevent integer overflow
- Calculates space for one HashInstrumentation structure per worker process
- Includes overhead for SharedHashInfo container structure in size calculation
- Registers exactly one key with the shared memory TOC system
- Part of the broader parallel query execution framework
- Memory estimation occurs during query planning, before actual parallel execution begins
- Essential for collecting distributed performance statistics across worker processes

## Simplified Source

```c
void ExecHashEstimate(HashState *node, ParallelContext *pcxt)
{
    size_t size;

    // Skip if no instrumentation enabled or no workers
    if (!node->ps.instrument || pcxt->nworkers == 0)
        return;

    // Calculate space for instrumentation data per worker
    size = mul_size(pcxt->nworkers, sizeof(HashInstrumentation));
    size = add_size(size, offsetof(SharedHashInfo, hinstrument));

    // Register memory requirements with shared memory estimator
    shm_toc_estimate_chunk(&pcxt->estimator, size);
    shm_toc_estimate_keys(&pcxt->estimator, 1);
}
```