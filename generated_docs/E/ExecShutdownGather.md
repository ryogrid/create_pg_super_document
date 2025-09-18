# ExecShutdownGather

## Location
[src/backend/executor/nodeGather.c:411-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGather.c#L411-L434)

## Overview
Completely shuts down a Gather node by stopping all parallel workers and destroying the parallel execution context.

## Definition
void ExecShutdownGather(GatherState *node)

## Detailed Description
ExecShutdownGather is the primary shutdown function for Gather executor nodes in PostgreSQL's parallel query execution system. It performs a complete teardown of parallel execution resources in two phases: first, it calls ExecShutdownGatherWorkers to stop all parallel worker processes and clean up communication resources, then it destroys the parallel context itself using ExecParallelCleanup. This function ensures that all parallel execution infrastructure is properly dismantled and resources are freed.

## Parameters / Member Variables
- : A pointer to the GatherState structure representing the Gather node to be shut down, containing parallel execution context and worker information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecShutdownGatherWorkers](ExecShutdownGatherWorkers.md)
  - [ExecParallelCleanup](ExecParallelCleanup.md)
- Called from (representative examples):
  - [ExecShutdownNode_walker](ExecShutdownNode_walker.md)
  - [ExecEndGather](ExecEndGather.md)

## Notes and Other Information
This is a public function exposed via nodeGather.h and is part of the executor node interface. It provides the standard shutdown mechanism for Gather nodes and is typically called during query cleanup or when the executor needs to shut down parallel operations. The function safely handles cases where no parallel context was established by checking for null pointers before cleanup.