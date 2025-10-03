# ExecCustomScanReInitializeDSM

## Location
[src/backend/executor/nodeCustom.c:190-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCustom.c#L190-L204)

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
  - [CustomScanState](../C/CustomScanState.md) (structure type)
  - [ParallelContext](../P/ParallelContext.md) (structure type)
  - [CustomExecMethods](../C/CustomExecMethods.md) (structure type)
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (shared memory TOC lookup function)
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md) (general parallel execution DSM re-initializer)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- If the custom scan provider does not implement ReInitializeDSMCustomScan, no re-initialization occurs
- The shared memory segment must have been previously allocated and initialized by ExecCustomScanInitializeDSM
- The plan_node_id is used as the key to locate the existing shared memory segment
- The coordinate pointer allows the custom scan provider to reset its shared state without reallocating memory
- This function is called when parallel execution needs to restart from the beginning
- The shm_toc_lookup call uses 'false' for the missing_ok parameter, meaning it will error if the segment is not found

## Simplified Source
```c
void ExecCustomScanReInitializeDSM(CustomScanState *node, ParallelContext *pcxt) {
    const CustomExecMethods *methods = node->methods;

    // Only reinitialize if the custom scan provider supports it
    if (methods->ReInitializeDSMCustomScan) {
        int plan_node_id = node->ss.ps.plan->plan_node_id;

        // Find the shared memory coordinate for this plan node
        void *coordinate = shm_toc_lookup(pcxt->toc, plan_node_id, false);

        // Delegate to the custom scan provider's reinitialization method
        methods->ReInitializeDSMCustomScan(node, pcxt, coordinate);
    }
}
```