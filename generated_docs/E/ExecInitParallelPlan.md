# ExecInitParallelPlan

## Location
[src/backend/executor/execParallel.c:587-877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L587-L877)

## Overview
ExecInitParallelPlan is the comprehensive initialization function that sets up all infrastructure required for parallel query execution, including shared memory allocation, tuple queues, instrumentation, and plan state preparation for worker processes.

## Definition

```c
ParallelExecutorInfo *
ExecInitParallelPlan(PlanState *planstate, EState *estate,
					 Bitmapset *sendParams, int nworkers,
					 int64 tuples_needed)
```
## Detailed Description
This function orchestrates the complete setup process for parallel query execution in PostgreSQL. It handles the complex task of creating and configuring the shared memory environment that enables coordination between the main backend process and multiple parallel worker processes.

The function operates in several phases:

1. **Parameter Evaluation**: Forces evaluation of any initplan parameters that need to be passed to workers
2. **Space Estimation**: Calculates memory requirements for all shared data structures including plan state, parameters, instrumentation, and tuple queues  
3. **DSM Creation**: Creates and initializes the dynamic shared memory segment
4. **Data Serialization**: Stores serialized query text, planned statement, parameters, and other execution state in shared memory
5. **Communication Setup**: Establishes tuple queues for result collection and data structures for resource usage tracking
6. **Plan Initialization**: Calls node-specific DSM initialization routines for parallel-aware plan nodes
7. **Instrumentation Setup**: Configures performance monitoring structures if enabled

The function ensures that all worker processes will have access to the complete execution context needed to execute their portion of the parallel plan and return results to the coordinator.

Key components initialized:
- Fixed execution state (tuple limits, flags, JIT settings)
- Query text and serialized plan
- Parameter lists and execution parameters
- Buffer and WAL usage tracking arrays
- Tuple queues for result communication
- Instrumentation data for performance analysis
- DSA (Dynamic Shared Area) for variable-size allocations

## Parameters / Member Variables
- `*planstate`: Root plan state node to be executed in parallel
- `*estate`: Execution state containing query context, parameters, and configuration
- `*sendParams`: Bitmapset identifying which execution parameters to send to workers
- `nworkers`: Number of parallel worker processes to create
- `tuples_needed`: Hint about expected number of result tuples (for optimization)
## Dependencies
- Functions called/Symbols referenced:
  - [ExecSetParamPlanMulti](ExecSetParamPlanMulti.md) (evaluate initplan parameters)
  - [ExecSerializePlan](ExecSerializePlan.md) (serialize plan for workers)
  - [CreateParallelContext](../C/CreateParallelContext.md) (create parallel execution context)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (create shared memory segment)
  - [ExecParallelSetupTupleQueues](ExecParallelSetupTupleQueues.md) (establish communication channels)
  - [ExecParallelEstimate](ExecParallelEstimate.md), ExecParallelInitializeDSM (plan node setup)
  - [SerializeParamList](../S/SerializeParamList.md), SerializeParamExecParams (parameter handling)
  - dsa_create_in_place (dynamic shared memory allocation)
  - Various shm_toc_* functions for shared memory table of contents management
- Called from:
  - [ExecGather](ExecGather.md) (Gather node initialization)
  - [ExecGatherMerge](ExecGatherMerge.md) (GatherMerge node initialization)

## Notes and Other Information
- Returns a ParallelExecutorInfo structure containing all parallel execution state
- The function performs extensive memory estimation before creating the DSM to ensure adequate space
- [Instrumentation](../I/Instrumentation.md) setup includes both general query instrumentation and JIT-specific instrumentation when enabled
- Parameter serialization uses DSA storage to handle varying parameter sizes across query executions
- The function validates consistency between estimation and initialization phases
- Creates a DSA area that can be used by both leader and worker processes for dynamic allocations
- All shared memory structures are registered in the table of contents with specific keys for worker discovery
- The function temporarily installs the DSA area in the estate during plan initialization to enable DSA-aware operations

## Simplified Source

```c
ParallelExecutorInfo *ExecInitParallelPlan(PlanState *planstate, EState *estate,
                                           Bitmapset *sendParams, int nworkers,
                                           int64 tuples_needed)
{
    ParallelExecutorInfo *pei;
    ParallelContext *pcxt;

    // Step 1: Force evaluation of initplan parameters for workers
    ExecSetParamPlanMulti(sendParams, GetPerTupleExprContext(estate));

    // Step 2: Create parallel executor info structure
    pei = palloc0(sizeof(ParallelExecutorInfo));
    pei->finished = false;
    pei->planstate = planstate;

    // Step 3: Serialize plan for worker processes
    char *serialized_plan = ExecSerializePlan(planstate->plan, estate);

    // Step 4: Create parallel context for managing workers
    pcxt = CreateParallelContext("postgres", "ParallelQueryMain", nworkers);
    pei->pcxt = pcxt;

    // Step 5: Estimate memory requirements for shared structures
    ExecParallelEstimateContext estimate_context;
    estimate_context.pcxt = pcxt;
    estimate_context.nnodes = 0;

    // Estimate space for all shared data structures
    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(FixedParallelExecutorState));
    shm_toc_estimate_chunk(&pcxt->estimator, strlen(estate->es_sourceText) + 1);
    shm_toc_estimate_chunk(&pcxt->estimator, strlen(serialized_plan) + 1);
    shm_toc_estimate_chunk(&pcxt->estimator, EstimateParamListSpace(estate->es_param_list_info));
    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(BufferUsage) * pcxt->nworkers);
    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(WalUsage) * pcxt->nworkers);
    shm_toc_estimate_chunk(&pcxt->estimator, PARALLEL_TUPLE_QUEUE_SIZE * pcxt->nworkers);

    // Let parallel-aware nodes add their estimates
    ExecParallelEstimate(planstate, &estimate_context);

    // Step 6: Create the dynamic shared memory segment
    InitializeParallelDSM(pcxt);

    // Step 7: Store all shared data in DSM
    // Store fixed execution state
    FixedParallelExecutorState *fpes = shm_toc_allocate(pcxt->toc, sizeof(FixedParallelExecutorState));
    fpes->tuples_needed = tuples_needed;
    fpes->eflags = estate->es_top_eflags;
    fpes->jit_flags = estate->es_jit_flags;
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_EXECUTOR_FIXED, fpes);

    // Store query text and serialized plan
    char *query_space = shm_toc_allocate(pcxt->toc, strlen(estate->es_sourceText) + 1);
    strcpy(query_space, estate->es_sourceText);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, query_space);

    char *plan_space = shm_toc_allocate(pcxt->toc, strlen(serialized_plan) + 1);
    strcpy(plan_space, serialized_plan);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_PLANNEDSTMT, plan_space);

    // Store parameters
    char *param_space = shm_toc_allocate(pcxt->toc, EstimateParamListSpace(estate->es_param_list_info));
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_PARAMLISTINFO, param_space);
    SerializeParamList(estate->es_param_list_info, &param_space);

    // Step 8: Set up communication infrastructure
    // Allocate resource tracking arrays
    pei->buffer_usage = shm_toc_allocate(pcxt->toc, sizeof(BufferUsage) * pcxt->nworkers);
    pei->wal_usage = shm_toc_allocate(pcxt->toc, sizeof(WalUsage) * pcxt->nworkers);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, pei->buffer_usage);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, pei->wal_usage);

    // Set up tuple queues for result collection
    pei->tqueue = ExecParallelSetupTupleQueues(pcxt, false);
    pei->reader = NULL;  // Created later when workers start

    // Step 9: Set up instrumentation if enabled
    if (estate->es_instrument)
    {
        // Allocate and initialize instrumentation structures
        size_t instr_size = sizeof(SharedExecutorInstrumentation) +
                           sizeof(Instrumentation) * estimate_context.nnodes * nworkers;
        SharedExecutorInstrumentation *instrumentation = shm_toc_allocate(pcxt->toc, instr_size);
        instrumentation->instrument_options = estate->es_instrument;
        instrumentation->num_workers = nworkers;
        instrumentation->num_plan_nodes = estimate_context.nnodes;
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_INSTRUMENTATION, instrumentation);
        pei->instrumentation = instrumentation;
    }

    // Step 10: Create DSA area and initialize parallel-aware nodes
    if (pcxt->seg != NULL)
    {
        char *dsa_space = shm_toc_allocate(pcxt->toc, dsa_minimum_size());
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_DSA, dsa_space);
        pei->area = dsa_create_in_place(dsa_space, dsa_minimum_size(),
                                        LWTRANCHE_PARALLEL_QUERY_DSA, pcxt->seg);

        // Serialize execution parameters using DSA storage
        if (!bms_is_empty(sendParams))
        {
            pei->param_exec = SerializeParamExecParams(estate, sendParams, pei->area);
            fpes->param_exec = pei->param_exec;
        }
    }

    // Step 11: Initialize parallel-aware plan nodes
    ExecParallelInitializeDSMContext init_context;
    init_context.pcxt = pcxt;
    init_context.instrumentation = pei->instrumentation;
    init_context.nnodes = 0;

    estate->es_query_dsa = pei->area;  // Temporarily install DSA
    ExecParallelInitializeDSM(planstate, &init_context);
    estate->es_query_dsa = NULL;

    return pei;  // Ready for parallel execution
}
```