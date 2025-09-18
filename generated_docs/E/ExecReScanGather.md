# ExecReScanGather

## Location
src/backend/executor/nodeGather.c: 435 - 469

## Overview
Prepares a Gather node for re-scanning by shutting down existing workers and resetting scan state for both parallel and child nodes.

## Definition
void ExecReScanGather(GatherState *node)

## Detailed Description
ExecReScanGather handles the complex task of preparing a Gather node for a rescan operation in PostgreSQL's parallel query execution system. It first shuts down any existing parallel workers to ensure a clean state, then marks the node as uninitialized so that shared state will be rebuilt on the next execution. The function carefully manages parameter change notifications to child nodes, particularly handling rescan parameters that indicate when the leader process might see a different subset of rows. It coordinates the rescan process between the parallel execution context and child plan nodes, ensuring proper ordering of reinitialization steps.

## Parameters / Member Variables
- : A pointer to the GatherState structure representing the Gather node to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - ExecShutdownGatherWorkers
  - outerPlanState
  - bms_add_member
  - ExecReScan
- Called from (representative examples):
  - ExecReScan (general executor rescan dispatcher)

## Notes and Other Information
This function is part of the executor node interface and is exposed via nodeGather.h. It handles the intricate coordination required when rescanning parallel operations, including proper management of parameter changes and the relationship between shared and local state. The function includes detailed comments about the ordering requirements between ReInitializeDSM, ReScan, and ExecProcNode calls, which is critical for parallel-aware child nodes. The rescan_param mechanism allows the Gather node to signal that the leader process subset might change even if the overall rowset remains the same.