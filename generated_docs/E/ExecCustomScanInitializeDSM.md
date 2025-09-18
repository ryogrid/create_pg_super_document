# ExecCustomScanInitializeDSM

## Location
[src/backend/executor/nodeCustom.c:174-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCustom.c#L174-L189)

## Overview
Initializes the dynamic shared memory (DSM) segment for a custom scan node in parallel query execution.

## Definition
```c
void ExecCustomScanInitializeDSM(CustomScanState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecCustomScanInitializeDSM is responsible for setting up the shared memory coordination structure for a custom scan node when executing in parallel mode. This function allocates a shared memory segment of the size previously estimated by ExecCustomScanEstimate, calls the custom scan provider's InitializeDSMCustomScan method to initialize the coordination data, and registers the allocated memory with the shared memory table of contents (TOC) using the plan node's unique identifier as the key. This allows parallel workers to later locate and access the shared coordination data.

## Parameters / Member Variables
- `node`: A pointer to the CustomScanState structure representing the custom scan node
- `pcxt`: A pointer to the ParallelContext structure containing parallel execution context and shared memory information

## Dependencies
- Functions called/Symbols referenced:
  - [CustomScanState](../C/CustomScanState.md) (structure type)
  - [ParallelContext](../P/ParallelContext.md) (structure type)
  - [CustomExecMethods](../C/CustomExecMethods.md) (structure type)
  - shm_toc_allocate (shared memory TOC allocation function)
  - shm_toc_insert (shared memory TOC insertion function)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (general parallel execution DSM initializer)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- If the custom scan provider does not implement InitializeDSMCustomScan, no shared memory initialization occurs
- The allocated memory size (pscan_len) must have been previously set by ExecCustomScanEstimate
- The plan_node_id serves as a unique key to identify this custom scan's shared memory segment
- The coordinate pointer passed to InitializeDSMCustomScan allows the custom scan provider to initialize its shared state
- This function runs in the leader process before parallel workers are launched