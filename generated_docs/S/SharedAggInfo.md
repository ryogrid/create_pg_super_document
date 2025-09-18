# SharedAggInfo

## Location
src/include/nodes/execnodes.h: 2438 - 2442

## Overview
SharedAggInfo is a shared memory container structure used to store per-worker aggregate information in parallel query execution within PostgreSQL's aggregation system.

## Definition


## Detailed Description
SharedAggInfo serves as a shared memory data structure that facilitates coordination and information sharing between multiple worker processes during parallel aggregate operations. It acts as a container for aggregate instrumentation data that needs to be accessible across different worker processes in a parallel query execution context. The structure uses a flexible array member to accommodate a variable number of AggregateInstrumentation entries, one for each worker process involved in the parallel aggregation.

## Parameters / Member Variables
- `num_workers`: The number of worker processes participating in the parallel aggregate operation
- `sinstrument`: A flexible array of AggregateInstrumentation structures, with one entry per worker process, containing performance and execution statistics for each worker's aggregate operations

## Dependencies
- Functions called/Symbols referenced:
  - AggregateInstrumentation
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - ExecAggEstimate
  - ExecAggInitializeDSM
  - ExecAggRetrieveInstrumentation
  - AggState (as a member)

## Notes and Other Information
- This structure is specifically designed for parallel query execution scenarios where multiple worker processes collaborate on aggregate operations
- The flexible array member allows the structure to be allocated with the exact number of instrumentation entries needed based on the actual number of workers
- Used in conjunction with PostgreSQL's dynamic shared memory (DSM) system for inter-process communication during parallel aggregation
- The structure is referenced in the AggState execution node, indicating its integration into the broader aggregation execution framework