# ExecInitGatherMerge

## Location
[src/backend/executor/nodeGatherMerge.c:67-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L67-L182)

## Overview
ExecInitGatherMerge initializes the execution state for a GatherMerge plan node, setting up the infrastructure needed to collect and merge-sort tuples from parallel worker processes.

## Definition
```c
GatherMergeState *ExecInitGatherMerge(GatherMerge *node, EState *estate, int eflags)
```

## Detailed Description
This function creates and initializes a GatherMergeState structure that manages the execution of a GatherMerge operation. The GatherMerge node is responsible for collecting sorted results from multiple parallel worker processes and merging them into a single sorted output stream. Unlike regular Gather nodes, GatherMerge maintains the sort order of the combined results.

The function performs several key initialization tasks:
- Creates the GatherMergeState structure and links it to the execution infrastructure
- Initializes the outer plan (the plan that will be executed by workers)
- Sets up tuple descriptors and projection information for handling results
- Configures sort key information needed for the merge operation
- Allocates workspace memory for the gather merge process

The initialization ensures that the node can handle tuples both from the leader process (if need_to_scan_locally is true) and from worker processes via tuple queues, requiring flexible slot type handling.

## Parameters / Member Variables
- `node`: The GatherMerge plan node containing configuration such as sort columns, operators, and collations
- `estate`: The execution state containing global execution context and parameters
- `eflags`: Execution flags that may affect initialization behavior

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecInitNode](ExecInitNode.md)
  - [ExecGetResultType](ExecGetResultType.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecConditionalAssignProjectionInfo](ExecConditionalAssignProjectionInfo.md)
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
  - [gather_merge_setup](../g/gather_merge_setup.md)
- Called from:
  - [ExecInitNode](ExecInitNode.md) (main executor initialization dispatcher)

## Notes and Other Information
- [GatherMerge](../G/GatherMerge.md) nodes do not support qual conditions as it is more efficient to apply them in child nodes
- The node does not have an inner plan, only an outer plan that gets executed by workers
- [Sort](../S/Sort.md) key initialization includes setting up SortSupport structures but explicitly disables abbreviated key conversion for consistency with MergeAppend behavior
- The function sets outeropsset=true and outeropsfixed=false to handle the mixed tuple sources (leader vs workers)
- Memory allocation for sort keys and workspace is performed in the current memory context

## Simplified Source

```c
GatherMergeState *
ExecInitGatherMerge(GatherMerge *node, EState *estate, int eflags)
{
    // Create and initialize the GatherMerge state structure
    GatherMergeState *gm_state = makeNode(GatherMergeState);
    gm_state->ps.plan = (Plan *) node;
    gm_state->ps.state = estate;
    gm_state->ps.ExecProcNode = ExecGatherMerge;

    // Initialize GatherMerge-specific state
    gm_state->initialized = false;
    gm_state->gm_initialized = false;
    gm_state->tuples_needed = -1;

    // Create expression context
    ExecAssignExprContext(estate, &gm_state->ps);

    // Initialize the outer plan
    Plan *outerNode = outerPlan(node);
    outerPlanState(gm_state) = ExecInitNode(outerNode, estate, eflags);

    // Set up slot operations for mixed tuple sources
    gm_state->ps.outeropsset = true;
    gm_state->ps.outeropsfixed = false;

    // Store tuple descriptor and initialize result type
    TupleDesc tupDesc = ExecGetResultType(outerPlanState(gm_state));
    gm_state->tupDesc = tupDesc;

    ExecInitResultTypeTL(&gm_state->ps);
    ExecConditionalAssignProjectionInfo(&gm_state->ps, tupDesc, OUTER_VAR);

    // Handle result operations when no projection
    if (gm_state->ps.ps_ProjInfo == NULL)
    {
        gm_state->ps.resultopsset = true;
        gm_state->ps.resultopsfixed = false;
    }

    // Initialize sort key information
    if (node->numCols)
    {
        gm_state->gm_nkeys = node->numCols;
        gm_state->gm_sortkeys = palloc0(sizeof(SortSupportData) * node->numCols);

        for (int i = 0; i < node->numCols; i++)
        {
            SortSupport sortKey = gm_state->gm_sortkeys + i;
            sortKey->ssup_cxt = CurrentMemoryContext;
            sortKey->ssup_collation = node->collations[i];
            sortKey->ssup_nulls_first = node->nullsFirst[i];
            sortKey->ssup_attno = node->sortColIdx[i];
            sortKey->abbreviate = false;  // No abbreviated keys for consistency with MergeAppend

            PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
        }
    }

    // Allocate workspace for gather merge
    gather_merge_setup(gm_state);

    return gm_state;
}
```