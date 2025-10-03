# ExecGatherMerge

## Location
[src/backend/executor/nodeGatherMerge.c:183-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L183-L283)

## Overview
ExecGatherMerge is the main execution function for GatherMerge nodes that scans relations via multiple parallel workers and returns the next qualifying tuple in sorted order.

## Definition
```c
static TupleTableSlot *ExecGatherMerge(PlanState *pstate)
```

## Detailed Description
ExecGatherMerge manages the execution of parallel query processing with sorted result merging. This function coordinates between multiple worker processes to execute a plan in parallel while maintaining sort order in the final output stream.

The function operates in two main phases:
1. **Initialization phase** (first call only): Sets up parallel workers, launches them, and establishes tuple queue readers for communication. It also determines whether the leader process should participate in scanning based on configuration and worker availability.

2. **Execution phase** (every call): Retrieves the next tuple from the merge process via gather_merge_getnext(), which handles the complex logic of merging sorted streams from multiple workers and the leader process.

Key features include:
- Lazy worker initialization (workers are launched only when the node is first executed)
- Support for both leader participation and worker-only execution
- Automatic fallback to sequential execution when parallel mode is disabled or no workers are available
- Expression context management for tuple processing
- Optional projection of results through ExecProject()

## Parameters / Member Variables
- `pstate`: The PlanState containing the GatherMergeState and execution context

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - CHECK_FOR_INTERRUPTS
  - [ExecInitParallelPlan](ExecInitParallelPlan.md)
  - [ExecParallelReinitialize](ExecParallelReinitialize.md)
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md)
  - [ExecParallelCreateReaders](ExecParallelCreateReaders.md)
  - ResetExprContext
  - [gather_merge_getnext](../g/gather_merge_getnext.md)
  - TupIsNull
  - [ExecProject](ExecProject.md)
- Called from:
  - [ExecInitGatherMerge](ExecInitGatherMerge.md) (set as the ExecProcNode function pointer)

## Notes and Other Information
- Workers are launched lazily on first execution rather than during initialization, allowing for better resource management
- The function handles both cases where workers are available and where the query must run sequentially
- Leader participation is controlled by the parallel_leader_participation GUC and worker availability
- Memory context is reset on each tuple to prevent memory leaks during long-running queries
- The actual merge logic is delegated to gather_merge_getnext(), keeping this function focused on coordination
- Returns NULL when no more tuples are available from any source
- Supports both projection and pass-through modes depending on whether ps_ProjInfo is configured

## Simplified Source

```c
static TupleTableSlot *
ExecGatherMerge(PlanState *pstate)
{
    GatherMergeState *node = castNode(GatherMergeState, pstate);
    TupleTableSlot *slot;
    ExprContext *econtext;

    CHECK_FOR_INTERRUPTS();

    // Initialize parallel execution on first call
    if (!node->initialized) {
        EState *estate = node->ps.state;
        GatherMerge *gm = castNode(GatherMerge, node->ps.plan);

        // Set up parallel workers if enabled
        if (gm->num_workers > 0 && estate->es_use_parallel_mode) {
            // Initialize or reinitialize parallel plan
            if (!node->pei)
                node->pei = ExecInitParallelPlan(outerPlanState(node), estate,
                                                gm->initParam, gm->num_workers,
                                                node->tuples_needed);
            else
                ExecParallelReinitialize(outerPlanState(node), node->pei, gm->initParam);

            // Launch workers and set up readers
            LaunchParallelWorkers(node->pei->pcxt);
            node->nworkers_launched = node->pei->pcxt->nworkers_launched;

            if (node->pei->pcxt->nworkers_launched > 0) {
                ExecParallelCreateReaders(node->pei);
                // Set up reader array for worker communication
                node->nreaders = node->pei->pcxt->nworkers_launched;
                node->reader = palloc(node->nreaders * sizeof(TupleQueueReader *));
                memcpy(node->reader, node->pei->reader,
                       node->nreaders * sizeof(TupleQueueReader *));
            }
        }

        // Enable leader participation if configured or no workers available
        if (parallel_leader_participation || node->nreaders == 0)
            node->need_to_scan_locally = true;
        node->initialized = true;
    }

    // Reset memory context per tuple
    econtext = node->ps.ps_ExprContext;
    ResetExprContext(econtext);

    // Get next sorted tuple from merge process
    slot = gather_merge_getnext(node);
    if (TupIsNull(slot))
        return NULL;

    // Apply projection if needed
    if (node->ps.ps_ProjInfo == NULL)
        return slot;

    econtext->ecxt_outertuple = slot;
    return ExecProject(node->ps.ps_ProjInfo);
}
```