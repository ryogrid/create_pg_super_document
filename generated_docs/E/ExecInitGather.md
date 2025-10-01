# ExecInitGather

## Location
[src/backend/executor/nodeGather.c:53-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGather.c#L53-L136)

## Overview
Initializes the execution state for a Gather plan node, which is responsible for coordinating parallel query execution by collecting results from multiple worker processes.

## Definition

```c
structure
	 */
	gatherstate = makeNode(GatherState);
```
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
  - [ExecAssignExprContext](ExecAssignExprContext.md) (sets up expression evaluation context)
  - [ExecInitNode](ExecInitNode.md) (initializes the outer child plan node)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple descriptor from child)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (initializes result type from target list)
  - [ExecConditionalAssignProjectionInfo](ExecConditionalAssignProjectionInfo.md) (sets up projection if needed)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md) (creates funnel slot for worker tuples)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (main node initialization dispatcher)

## Notes and Other Information
- [Gather](../G/Gather.md) nodes do not have inner plan nodes and this is verified with an assertion
- The function sets outeropsfixed to false because tuples may come from different sources with potentially different slot implementations
- A funnel_slot is created specifically for collecting tuples from worker processes using minimal tuple operations for efficiency
- [Gather](../G/Gather.md) nodes do not support qual conditions as it's more efficient to apply filtering in child nodes
- The need_to_scan_locally flag determines whether the leader process participates in actual data scanning alongside coordinating workers

## Simplified Source

```c
GatherState *
ExecInitGather(Gather *node, EState *estate, int eflags)
{
    // Create and initialize the Gather state structure
    GatherState *gatherstate = makeNode(GatherState);
    gatherstate->ps.plan = (Plan *) node;
    gatherstate->ps.state = estate;
    gatherstate->ps.ExecProcNode = ExecGather;

    // Initialize Gather-specific state
    gatherstate->initialized = false;
    gatherstate->need_to_scan_locally = !node->single_copy && parallel_leader_participation;
    gatherstate->tuples_needed = -1;

    // Create expression context
    ExecAssignExprContext(estate, &gatherstate->ps);

    // Initialize the outer plan
    Plan *outerNode = outerPlan(node);
    outerPlanState(gatherstate) = ExecInitNode(outerNode, estate, eflags);
    TupleDesc tupDesc = ExecGetResultType(outerPlanState(gatherstate));

    // Set up slot operations for mixed tuple sources
    gatherstate->ps.outeropsset = true;
    gatherstate->ps.outeropsfixed = false;

    // Initialize result type and projection
    ExecInitResultTypeTL(&gatherstate->ps);
    ExecConditionalAssignProjectionInfo(&gatherstate->ps, tupDesc, OUTER_VAR);

    // Handle result operations when no projection
    if (gatherstate->ps.ps_ProjInfo == NULL)
    {
        gatherstate->ps.resultopsset = true;
        gatherstate->ps.resultopsfixed = false;
    }

    // Create funnel slot for collecting tuples from workers
    gatherstate->funnel_slot = ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsMinimalTuple);

    return gatherstate;
}
```