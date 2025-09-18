# ExecInitParallelPlan

## Location
src/backend/executor/execParallel.c: 587 - 877

## Overview
ExecInitParallelPlan is the comprehensive initialization function that sets up all infrastructure required for parallel query execution, including shared memory allocation, tuple queues, instrumentation, and plan state preparation for worker processes.

## Definition


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
- : Root plan state node to be executed in parallel
- : Execution state containing query context, parameters, and configuration
- : Bitmapset identifying which execution parameters to send to workers
- : Number of parallel worker processes to create
- : Hint about expected number of result tuples (for optimization)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecSetParamPlanMulti](ExecSetParamPlanMulti.md) (evaluate initplan parameters)
  - ExecSerializePlan (serialize plan for workers)
  - [CreateParallelContext](../C/CreateParallelContext.md) (create parallel execution context)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (create shared memory segment)
  - [ExecParallelSetupTupleQueues](ExecParallelSetupTupleQueues.md) (establish communication channels)
  - ExecParallelEstimate, ExecParallelInitializeDSM (plan node setup)
  - [SerializeParamList](../S/SerializeParamList.md), SerializeParamExecParams (parameter handling)
  - dsa_create_in_place (dynamic shared memory allocation)
  - Various shm_toc_* functions for shared memory table of contents management
- Called from:
  - [ExecGather](ExecGather.md) (Gather node initialization)
  - [ExecGatherMerge](ExecGatherMerge.md) (GatherMerge node initialization)

## Notes and Other Information
- Returns a ParallelExecutorInfo structure containing all parallel execution state
- The function performs extensive memory estimation before creating the DSM to ensure adequate space
- Instrumentation setup includes both general query instrumentation and JIT-specific instrumentation when enabled
- Parameter serialization uses DSA storage to handle varying parameter sizes across query executions
- The function validates consistency between estimation and initialization phases
- Creates a DSA area that can be used by both leader and worker processes for dynamic allocations
- All shared memory structures are registered in the table of contents with specific keys for worker discovery
- The function temporarily installs the DSA area in the estate during plan initialization to enable DSA-aware operations