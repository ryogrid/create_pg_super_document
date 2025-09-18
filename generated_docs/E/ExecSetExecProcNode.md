# ExecSetExecProcNode

## Location
[src/backend/executor/execProcnode.c:425-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execProcnode.c#L425-L442)

## Overview
ExecSetExecProcNode sets or changes the execution procedure function for a plan node, installing necessary wrapper functions for stack depth checking and instrumentation.

## Definition


## Detailed Description
ExecSetExecProcNode provides a mechanism for plan nodes to change their execution function after initialization is complete. This is particularly important for nodes that may need to switch execution strategies based on runtime conditions or for nodes that require different execution procedures during their lifecycle.

The function implements a wrapper system that ensures proper execution environment setup:
- It stores the actual execution function in ExecProcNodeReal
- It sets ExecProcNode to ExecProcNodeFirst, which acts as a first-time execution wrapper
- ExecProcNodeFirst handles stack depth checking and instrumentation setup before delegating to the real execution function

This design allows nodes to change their execution behavior while maintaining the infrastructure for performance monitoring and stack overflow protection. The wrapper approach ensures that even if a node changes its execution function mid-execution, the proper safeguards remain in place.

## Parameters / Member Variables
- : The PlanState node whose execution function is being set
- : The ExecProcNodeMtd function pointer to be installed as the execution procedure

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProcNodeFirst](ExecProcNodeFirst.md) (first-time execution wrapper)
  - ExecProcNode (execution procedure field)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (during plan node initialization)
  - ExecHashJoinInitializeDSM (parallel hash join setup)
  - [ExecHashJoinInitializeWorker](ExecHashJoinInitializeWorker.md) (parallel worker initialization)
  - EvalPlanQualSetSlot (EPQ slot management)

## Notes and Other Information
- This function can be called after ExecInitNode() has finished to change execution behavior
- The wrapper system ensures stack checking and instrumentation remain active even when execution functions change
- If called after execution has begun, ExecProcNodeFirst may be executed superfluously, but this is considered acceptable overhead
- The function is essential for nodes that need dynamic execution strategy switching