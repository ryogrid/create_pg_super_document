# ExecCustomScanReInitializeDSM

## Location
src/backend/executor/nodeCustom.c: 190 - 204

## Overview
Re-initializes the dynamic shared memory (DSM) segment for a custom scan node when restarting parallel query execution.

## Definition
```c
void ExecCustomScanReInitializeDSM(CustomScanState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecCustomScanReInitializeDSM is responsible for re-initializing the shared memory coordination structure for a custom scan node when a parallel query is being restarted or reset. This function locates the previously allocated shared memory segment using the plan node's unique identifier, then calls the custom scan provider's ReInitializeDSMCustomScan method to reset the coordination data to its initial state. This is typically used when a parallel query needs to be re-executed, such as when rescanning is required or when recovering from certain error conditions.

## Parameters / Member Variables
- `node`: A pointer to the CustomScanState structure representing the custom scan node
- `pcxt`: A pointer to the ParallelContext structure containing parallel execution context and shared memory information

## Dependencies
- Functions called/Symbols referenced:
  - CustomScanState (structure type)
  - ParallelContext (structure type)
  - CustomExecMethods (structure type)
  - shm_toc_lookup (shared memory TOC lookup function)
- Called from (representative examples):
  - ExecParallelReInitializeDSM (general parallel execution DSM re-initializer)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- If the custom scan provider does not implement ReInitializeDSMCustomScan, no re-initialization occurs
- The shared memory segment must have been previously allocated and initialized by ExecCustomScanInitializeDSM
- The plan_node_id is used as the key to locate the existing shared memory segment
- The coordinate pointer allows the custom scan provider to reset its shared state without reallocating memory
- This function is called when parallel execution needs to restart from the beginning
- The shm_toc_lookup call uses 'false' for the missing_ok parameter, meaning it will error if the segment is not found