# ExecAppendEstimate

## Location
src/backend/executor/nodeAppend.c: 484 - 502

## Overview
Estimates the amount of shared memory space needed for parallel execution of an Append node in PostgreSQL's parallel query framework.

## Definition


## Detailed Description
ExecAppendEstimate is part of PostgreSQL's parallel query execution framework, specifically responsible for calculating the shared memory requirements for parallel Append node execution. The function is called during the parallel query planning phase to determine how much space to allocate in the Dynamic Shared Memory (DSM) segment.

The function performs two key calculations:

1. **Memory Size Calculation**: Computes the size needed for a ParallelAppendState structure, which includes a base structure plus a boolean array tracking completion status for each subplan
2. **Shared Memory Registration**: Registers the memory requirements with the parallel context's estimator, including both the data chunk and one table-of-contents key

The calculated space will be used to store synchronization information between parallel workers executing different subplans of the Append node.

## Parameters / Member Variables
- : The AppendState containing information about the number of subplans
- : The ParallelContext containing the shared memory estimator

## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md) (for safe size arithmetic preventing overflow)
  - offsetof (for calculating structure member offsets)
  - shm_toc_estimate_chunk (for registering data chunk memory needs)
  - shm_toc_estimate_keys (for registering table-of-contents key needs)
  - [ParallelAppendState](../P/ParallelAppendState.md) (the structure type being sized)
- Called from (representative examples):
  - ExecParallelEstimate (main parallel execution estimator dispatcher)

## Notes and Other Information
- The function calculates space for a ParallelAppendState which includes a boolean array sized according to the number of subplans
- Memory estimation is critical for parallel query performance as incorrect estimates can lead to memory allocation failures
- The function registers exactly one table-of-contents key for the parallel state data
- This is part of the parallel query infrastructure and only relevant for queries that can benefit from parallel execution
- The estimated memory will be used to coordinate which subplans have been completed by parallel workers