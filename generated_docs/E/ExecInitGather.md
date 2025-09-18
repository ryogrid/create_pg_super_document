# ExecInitGather

## Location
src/backend/executor/nodeGather.c: 53 - 136

## Overview
Initializes the execution state for a Gather plan node, which is responsible for coordinating parallel query execution by collecting results from multiple worker processes.

## Definition


## Detailed Description
ExecInitGather sets up the runtime state structure (GatherState) for a Gather plan node, which implements PostgreSQL's parallel query execution coordinator. The Gather node collects tuples from multiple parallel worker processes and optionally from the leader process itself. This function initializes all necessary data structures including the expression context, result tuple descriptor, projection information, and a special funnel slot used for tuple collection from workers.

The function determines whether the leader process should participate in scanning (need_to_scan_locally) based on the single_copy flag and the parallel_leader_participation setting. It also sets up slot operations to handle the fact that tuples may come from different sources (local execution or worker queues), requiring flexible slot type handling.

## Parameters / Member Variables
- : The Gather plan node containing configuration information including single_copy flag
- : The execution state containing global executor information and memory contexts
- : Execution flags controlling initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates GatherState structure)
  - ExecAssignExprContext (sets up expression evaluation context)
  - ExecInitNode (initializes the outer child plan node)
  - ExecGetResultType (gets result tuple descriptor from child)
  - ExecInitResultTypeTL (initializes result type from target list)
  - ExecConditionalAssignProjectionInfo (sets up projection if needed)
  - ExecInitExtraTupleSlot (creates funnel slot for worker tuples)
- Called from (representative examples):
  - ExecInitNode (main node initialization dispatcher)

## Notes and Other Information
- Gather nodes do not have inner plan nodes and this is verified with an assertion
- The function sets outeropsfixed to false because tuples may come from different sources with potentially different slot implementations
- A funnel_slot is created specifically for collecting tuples from worker processes using minimal tuple operations for efficiency
- Gather nodes do not support qual conditions as it's more efficient to apply filtering in child nodes
- The need_to_scan_locally flag determines whether the leader process participates in actual data scanning alongside coordinating workers