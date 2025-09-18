# ExecAggInitializeWorker

## Location
src/backend/executor/nodeAgg.c: 4729 - 4741

## Overview
This function attaches a worker process to DSM (Dynamic Shared Memory) space for aggregate statistics in PostgreSQL's parallel query execution framework.

## Definition


## Detailed Description
ExecAggInitializeWorker is responsible for initializing a parallel worker's access to shared aggregate state information. It performs the critical task of connecting a worker process to the shared memory segment that contains aggregate statistics and state. This function is part of PostgreSQL's parallel execution infrastructure, specifically for aggregate operations that can be parallelized across multiple worker processes.

The function uses the shared memory table of contents (TOC) to locate the appropriate shared information based on the plan node ID, enabling workers to coordinate their aggregate computations with the leader process and other workers.

## Parameters / Member Variables
- : Pointer to the AggState structure representing the aggregate node being initialized
- : Pointer to the ParallelWorkerContext containing shared memory information and the TOC for locating shared data

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
- Data types referenced:
  - AggState
  - ParallelWorkerContext
- Called from (representative examples):
  - ExecParallelInitializeWorker

## Notes and Other Information
- This function is specifically designed for parallel query execution scenarios
- The shared_info field in the AggState node is set to point to the shared memory segment
- The plan_node_id is used as the key to locate the correct shared information in the TOC
- This initialization is essential for workers to participate in parallel aggregate operations
- The function assumes that the DSM segment and TOC have been properly set up by the leader process